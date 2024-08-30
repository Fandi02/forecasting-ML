import dash
from dash import dcc, html
import plotly.graph_objs as go
from Prediction.sales_prediction import sales_prediction
from Prediction.stock_prediction import stock_prediction

data_sales = sales_prediction("EMP-444-0001", 15)
data_product = stock_prediction("74559", 15)

if data_sales is None:
    print("Not found")
    exit()

if data_product is None:
    print("Not found")
    exit()

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1('Data DMS Predictions'),
    dcc.Graph(
        id='sales-graph',
        figure={
            'data': [
                go.Scatter(
                    x=data_sales.doc_draft['ds'],
                    y=data_sales.doc_draft['y'],
                    mode='lines+markers',
                    name='Document DO Draft'
                ),
                go.Scatter(
                    x=data_sales.doc_void['ds'],
                    y=data_sales.doc_void['y'],
                    mode='lines+markers',
                    name='Document DO Void'
                ),
                go.Scatter(
                    x=data_sales.doc_applied['ds'],
                    y=data_sales.doc_applied['y'],
                    mode='lines+markers',
                    name='Document DO Applied'
                )
            ],
            'layout': go.Layout(
                title=f'Sales {data_sales.employee_id[0]}({data_sales.employee_name[0]}) Predictions',
                xaxis={'title': 'Days'},
                yaxis={'title': 'Predictions'}
            )
        }
    ),

    dcc.Graph(
        id='product-graph',
        figure={
            'data': [
                go.Scatter(
                    x=data_product.product_warehouse['ds'],
                    y=data_product.product_warehouse['y'],
                    mode='lines+markers',
                    name='Product in warehouse'
                ),
                go.Scatter(
                    x=data_product.product_employee['ds'],
                    y=data_product.product_employee['y'],
                    mode='lines+markers',
                    name='Product in employee'
                ),
                go.Scatter(
                    x=data_product.product_customer['ds'],
                    y=data_product.product_customer['y'],
                    mode='lines+markers',
                    name='Product in customer'
                )
            ],
            'layout': go.Layout(
                title=f'Product {data_product.product_id[0]}({data_product.product_name[0]}) Predictions. Unit: {data_product.unit_stock[0]}',
                xaxis={'title': 'Days'},
                yaxis={'title': 'Predictions'}
            )
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)