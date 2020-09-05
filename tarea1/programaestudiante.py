from functions_programa import load_CSV, join_datasets, aggregate_datasets, sort_by_notes

# Cargar CSV
dataframe_estudiantes, dataframe_curso, dataframe_notas = load_CSV()
# dataframe_estudiantes.show()
# dataframe_curso.show()
# dataframe_notas.show()

joined_df = join_datasets(dataframe_estudiantes,dataframe_curso,dataframe_notas)
# joined_df.show()

df_aggregate = aggregate_datasets(joined_df)
# df_aggregate.show()

top_10 = sort_by_notes(df_aggregate, 10)
top_10.show()

