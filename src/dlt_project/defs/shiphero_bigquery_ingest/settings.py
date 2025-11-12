DATA_ENDPOINT = "https://public-api.shiphero.com/graphql"
AUTH_ENDPOINT = "https://public-api.shiphero.com/auth/token"

# MAX_TIME = 600 # For testing
MAX_TIME = 1200 # For cloud functions
# MAX_TIME = 84600 # For Virtual Machines

ORDERS_LIMIT = 10 # For testing
# ORDERS_LIMIT = 500 # For production

LINE_ITEMS_COMPLEXITY = 102

ORDERS_QUERY = """
  query orders (
      $updated_to: ISODateTime,
      $updated_from: ISODateTime,
      $analyze: Boolean,
      $first: Int
    ) { 
    orders (
      updated_to: $updated_to,
      updated_from: $updated_from,
      analyze: $analyze
    ) { 
      complexity 
      request_id 
      data (first: $first) { 
        edges { 
          node { 
            id 
            order_number 
            partner_order_id
            source
            shop_name 
            fulfillment_status 
            order_date 
            total_tax 
            subtotal 
            total_discounts 
            total_price 
            updated_at 
            created_at 
            required_ship_date
            currency

            returns {
              id
              partner_id
              reason
              status
              label_type
              label_cost
              cost_to_customer
              shipping_carrier
              shipping_method 
              dimensions {
                weight
                length
                height
                width
              }
              total_items_expected 
              total_items_received 
              total_items_restocked 
              created_at 
              display_issue_refund 
              
            }           
            shipments { 
              id 
              created_date
              pending_shipment_id 
              total_packages 
              shipping_labels { 
                id 
                box_name 
                status 
                cost
                created_date
                carrier 
                shipping_name 
                shipping_method 
                partner_fulfillment_id 
                source 
                dimensions {
                  weight
                  height
                  width
                  length
                }
                package_number 
                tracking_status 
              }
            }
          } 
        } 
      } 
    } 
  } 
"""

LINE_ITEMS_QUERY = """
  query order(
    $id: String!
  ) {
    order(
      id: $id
    ) {
      request_id
      complexity
      data {
        id
        line_items {
          edges {
            node {
                id
                sku 
                partner_line_item_id 
                quantity 
                price
                fulfillment_status 
                quantity_pending_fulfillment 
                quantity_shipped 
                quantity_allocated 
                eligible_for_return 
                subtotal 
                created_at 
                updated_at 
                promotion_discount 
            }
          }
        }
      }
    }
  }
"""