from functions_programa import load_json_to_df, export_to_csv,format_df, get_total_products, get_total_sales, create_metrics

json_df = load_json_to_df()

df = format_df(json_df)

t_products = get_total_products(df)
export_to_csv(t_products, 'total_productos', True)
t_products.show()

t_sales = get_total_sales(df)
export_to_csv(t_sales, 'total_ventas', True)
t_sales.show()

metrics = create_metrics(df)
export_to_csv(metrics, 'metricas', True)
metrics.show()
