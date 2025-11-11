import time
from datetime import datetime, timezone

import dlt
import requests
import dpath

from . import settings


def get_shiphero_access_token(username: str, password: str) -> str:
    print(f"🔑 Fetching new access token for user: {username}")
    response = requests.post(
        settings.AUTH_ENDPOINT,
        json={"username": username, "password": password},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    token = payload.get("access_token") or payload.get("token")
    if not token:
        raise ValueError("ShipHero token response missing 'access_token'")
    print(f"✅ Access token obtained, length: {len(token)}")
    return token


def process_extensions(extensions):

    complexity = dpath.values(
        extensions,
        "throttling/estimated_complexity"
        )[0]

    credits = dpath.values(
        extensions,
        "throttling/user_quota/credits_remaining"
        )[0]

    increment_rate = dpath.values(
        extensions,
        "throttling/user_quota/increment_rate"
        )[0]

    return complexity, credits, increment_rate


def get_orders_data(access_token, since):

    last_hour = str(datetime.now(timezone.utc).replace(
                minute=0, second=0, microsecond=0))
    last_hour = last_hour[:19].replace(" ", "T")
    print(f"Gathering data up to: {last_hour}")

    payload = {
        "query": settings.ORDERS_QUERY,
        "variables": {
            "updated_from": since,
            "updated_to": last_hour,
            "analyze": False,
            "first": settings.ORDERS_LIMIT
        }
    }
    response = requests.post(
        settings.DATA_ENDPOINT,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json=payload
    )
    
    data = dpath.values(
        response.json(),
        "data/orders/data/edges/*/node"
    )

    order_count = dpath.values(
        response.json(),
        "extensions/throttling/cost_detail/orders/items_count"
    )
    print(f"Orders retrieved by the GraphQL query: {order_count}")

    complexity, credits, increment_rate = process_extensions(
        response.json()["extensions"]
    )

    return data, complexity, credits, increment_rate


def get_order_items(access_token, order_id):

    payload = {
        "query": settings.LINE_ITEMS_QUERY,
        "variables": {
            "id": order_id
        }
    }

    response = requests.post(
        settings.DATA_ENDPOINT,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60
    )

    data = dpath.values(
        response.json(),
        "data/order/data/line_items/edges/*/node"
        )

    complexity, credits, increment_rate = process_extensions(
        response.json()["extensions"]
    )

    return data, complexity, credits, increment_rate


def query_shiphero(access_token, query, since):

    print(f"Latest cursor value: {since}")

    orders, complexity, credits, increment_rate = get_orders_data(
        access_token,
        since
        )

    if orders == []:
        return []
    
    dates = [ x["updated_at"] for x in orders ]
    print(f"Freshest data gathered: {max(dates)}")

    if since == max(dates):
        # No fresher data available; signal the resource to end gracefully
        return []

    complexity = settings.LINE_ITEMS_COMPLEXITY

    for i, order in enumerate(orders):

        if credits < complexity:
            time.sleep(complexity/increment_rate*1.5)

        data, complexity, credits, increment_rate = get_order_items(
            access_token,
            order["id"]
            )

        orders[i]["line_items"] = data

    return orders

@dlt.resource(
    name="orders",
    primary_key="id",
    write_disposition="merge",
    max_table_nesting=0
)
def get_orders(
    username: str,
    password: str,
    access_token: str,
    updated_at = dlt.sources.incremental(
        "updated_at",
        initial_value="2025-08-01T00:00:00+00:00"
    )
):
    data = query_shiphero(
        access_token,
        settings.ORDERS_QUERY,
        since=updated_at.start_value,
        )
    if not data:
        # Nothing new to load; end the generator without error
        return

    yield from data

@dlt.source
def shiphero_source():

    username = dlt.secrets["sources.shiphero.username"]
    password = dlt.secrets["sources.shiphero.password"]
    access_token = get_shiphero_access_token(username, password)

    data = get_orders(username, password, access_token)
    if not data:
        return None
    return [data]

# @dlt.source
# def my_source():
#     @dlt.resource
#     def hello_world():
#         yield "hello, world!"

#     return hello_world

source = shiphero_source()
pipeline = dlt.pipeline(
        # pipeline_name='shiphero_pipeline',
        # source=source,
        destination='bigquery',
        dataset_name='dagster_shiphero',
    )