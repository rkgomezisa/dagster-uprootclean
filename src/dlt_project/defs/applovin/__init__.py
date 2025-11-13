"""
Axon Growth Reporting API Pipeline - Last 7 Days
Extracts advertising campaign performance data to BigQuery applovin_dlt_staging_v1 dataset
"""
import dlt
import requests
import time
import os
import json
from datetime import datetime, timedelta
from dagster import Definitions, asset, AssetExecutionContext, ScheduleDefinition, DefaultScheduleStatus


# ============================================================================
# SETUP
# ============================================================================
def setup_bigquery():
    """Configure BigQuery from BQ_KEY env var."""
    if bq_key := os.getenv("BQ_KEY"):
        try:
            creds = json.loads(bq_key)
            
            # Ensure the private key has proper newlines
            private_key = creds["private_key"]
            if "\\n" in private_key:
                private_key = private_key.replace("\\n", "\n")
            
            os.environ["CREDENTIALS__PROJECT_ID"] = creds["project_id"]
            os.environ["CREDENTIALS__PRIVATE_KEY"] = private_key
            os.environ["CREDENTIALS__CLIENT_EMAIL"] = creds["client_email"]
            
            print(f"✅ BigQuery credentials configured for project: {creds['project_id']}")
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse BQ_KEY as JSON: {e}")
        except KeyError as e:
            raise ValueError(f"Missing required credential field: {e}")
    else:
        raise ValueError("BQ_KEY environment variable not found")


# ============================================================================
# AXON GROWTH API FUNCTIONS
# ============================================================================
def handle_rate_limit(response, context=None):
    """Handle API rate limiting."""
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        msg = f"⏳ Rate limited, waiting {retry_after}s..."
        if context:
            context.log.warning(msg)
        else:
            print(msg)
        time.sleep(retry_after)
        return True
    return False


def get_advertiser_data(start_date: str, end_date: str, context=None):
    """
    Fetch advertiser campaign performance data from Axon Growth Reporting API.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        context: Optional Dagster context for logging
    
    Returns:
        List of performance records
    """
    api_key = os.getenv("APPLOVIN_API_KEY")
    if not api_key:
        raise ValueError("APPLOVIN_API_KEY environment variable not found")
    
    # Axon Growth Reporting API endpoint
    url = "https://r.applovin.com/report"
    
    # Define columns matching your BigQuery schema
    columns = [
        "day",
        "hour",
        "campaign",
        "campaign_id",
        "ad",
        "ad_id",
        "ad_type",
        "ad_creative_type",
        "creative_set",
        "creative_set_id",
        "impressions",
        "clicks",
        "ctr",
        "conversions",
        "cost",
        "sales",
        "roas_0d",
        "roas_7d",
        "chka_0d",
        "chka_7d",
        "chka_usd_0d",
        "chka_usd_7d",
        "cost_per_chka_0d",
        "cost_per_chka_7d",
    ]
    
    params = {
        "api_key": api_key,
        "start": start_date,
        "end": end_date,
        "format": "json",
        "columns": ",".join(columns),
    }
    
    try:
        if context:
            context.log.info(f"📡 Calling Axon Growth API: {start_date} to {end_date}")
            context.log.info(f"📋 Columns requested: {len(columns)} columns")
        
        response = requests.get(
            url,
            params=params,
            timeout=120
        )
        
        # Handle rate limiting
        if handle_rate_limit(response, context):
            return get_advertiser_data(start_date, end_date, context)
        
        # Log the response
        if context:
            context.log.info(f"📊 Response Status: {response.status_code}")
        
        # Handle errors with detailed logging
        if response.status_code in [400, 403, 404]:
            error_text = response.text[:1000]
            if context:
                context.log.error(f"❌ HTTP {response.status_code} Error")
                context.log.error(f"Response: {error_text}")
                context.log.error(f"Full URL: {response.url}")
            raise Exception(f"API Error {response.status_code}: {error_text}")
        
        response.raise_for_status()
        data = response.json()
        
        # Extract results - try different possible keys
        results = data.get("results", []) or data.get("data", []) or data.get("rows", [])
        
        if context:
            context.log.info(f"✅ Fetched {len(results)} records")
            
            # Log structure of first record
            if results:
                context.log.info(f"📋 Available fields: {list(results[0].keys())}")
        
        return results
        
    except requests.exceptions.RequestException as e:
        if context:
            context.log.error(f"❌ Request failed: {e}")
        raise


def transform_record(record):
    """
    Transform API response to match BigQuery schema.
    Ensures all fields from your schema are present.
    """
    return {
        # Time dimensions
        "day": record.get("day"),
        "hour": record.get("hour"),
        
        # Campaign info
        "campaign": record.get("campaign"),
        "campaign_id_external": record.get("campaign_id"),
        
        # Ad info
        "ad": record.get("ad"),
        "ad_id": record.get("ad_id"),
        "ad_type": record.get("ad_type"),
        "ad_creative_type": record.get("ad_creative_type"),
        
        # Creative info
        "creative_set": record.get("creative_set"),
        "creative_set_id": record.get("creative_set_id"),
        
        # Performance metrics
        "impressions": record.get("impressions"),
        "clicks": record.get("clicks"),
        "ctr": record.get("ctr"),
        "conversions": record.get("conversions"),
        
        # Financial metrics
        "cost": record.get("cost"),
        "sales": record.get("sales"),
        
        # ROAS metrics
        "roas_0d": record.get("roas_0d"),
        "roas_7d": record.get("roas_7d"),
        
        # CHKA (cohort) metrics - 0 day
        "chka_0d": record.get("chka_0d"),
        "chka_usd_0d": record.get("chka_usd_0d"),
        "cost_per_chka_0d": record.get("cost_per_chka_0d"),
        
        # CHKA (cohort) metrics - 7 day
        "chka_7d": record.get("chka_7d"),
        "chka_usd_7d": record.get("chka_usd_7d"),
        "cost_per_chka_7d": record.get("cost_per_chka_7d"),
        
        # Date range metadata
        "Start_Date": record.get("day"),
        "End_Date": record.get("day"),
    }


# ============================================================================
# DLT SOURCE
# ============================================================================
@dlt.source
def applovin_source(context=None):
    """Axon Growth advertising data source - Last 7 Days."""
    
    @dlt.resource(
        name="AppLovin_Ecommerce_Advertiser",
        primary_key=["day", "ad_id", "campaign_id_external"],
        write_disposition="replace",  # Replace all data each run since we're only doing L7D
        max_table_nesting=0
    )
    def advertiser_data():
        """Fetch last 7 days of advertising campaign performance data."""
        
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        # Calculate L7D date range
        end_date = datetime.now() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=6)  # 7 days ago (including yesterday)
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        log(f"🚀 Fetching Last 7 Days: {start_str} to {end_str}")
        
        try:
            raw_data = get_advertiser_data(start_str, end_str, context)
            
            if raw_data:
                # Transform records to match schema
                transformed_data = [transform_record(record) for record in raw_data]
                
                log(f"📦 Retrieved {len(transformed_data)} records for L7D")
                log(f"📅 Date range: {start_str} to {end_str}")
                
                yield transformed_data
            else:
                log(f"⚠️ No data available for L7D")
            
        except Exception as e:
            log(f"❌ Error fetching L7D data: {e}")
            raise
        
        log(f"🎉 L7D extraction complete!")
    
    return [advertiser_data()]


# ============================================================================
# DAGSTER
# ============================================================================
@asset(group_name="applovin")
def applovin_advertiser_data(context: AssetExecutionContext):
    """Extract Last 7 Days of Axon Growth advertising data to BigQuery applovin_dlt_staging_v1 dataset."""
    context.log.info("🔧 Setting up BigQuery credentials")
    setup_bigquery()
    
    context.log.info("🔐 Validating Axon API key")
    api_key = os.getenv("APPLOVIN_API_KEY")
    if not api_key:
        raise ValueError("APPLOVIN_API_KEY not found in environment variables")
    context.log.info(f"✅ API key found")
    
    context.log.info("🏗️ Creating DLT pipeline")
    pipeline = dlt.pipeline(
        pipeline_name="applovin_advertiser_pipeline",
        destination="bigquery",
        dataset_name="applovin_dlt_staging_v1",
    )
    
    context.log.info("🚀 Starting pipeline run - Last 7 Days")
    result = pipeline.run(applovin_source(context=context))
    context.log.info("✅ Pipeline run completed successfully!")
    context.log.info(f"📊 Load info: {result}")
    
    return result


defs = Definitions(
    assets=[applovin_advertiser_data],
    schedules=[
        ScheduleDefinition(
            name="applovin_advertiser_daily",
            target=applovin_advertiser_data,
            cron_schedule="0 3 * * *",  # Daily at 3 AM - fetches L7D each time
            default_status=DefaultScheduleStatus.STOPPED,
        )
    ],
)