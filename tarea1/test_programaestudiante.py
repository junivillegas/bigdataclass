from .programaestudiante import join_datasets, aggregate_datasets


# Data set notas sin estudiantes asignados
def data_good(spark_session):
    estudiantes_data = [
        (99324,'Sandor Gowing','Comunicación'),
        (89598,'Chev Litchmore','Comunicación'),
        (71910,'Puff Van der Merwe','Sociología'),
        (84646,'Lilias Brittan','Sociología'),
        (10963,'Saundra Doble','Periodismo')]

    dataframe_estudiantes = spark_session.createDataFrame(estudiantes_data,
                                               ['carnet', 'nombre', 'carrera'])
    curso_data = [
        (93,6,'Filantropía'),
        (29,2,'Biología'),
        (95,9,'Sociología'),
        (75,4,'Historia')]

    dataframe_curso = spark_session.createDataFrame(curso_data,
                                               ['codigo', 'creditos', 'nombre'])

    nota_data = [
        (99324,93,41),
        (89598,29,63),
        (71910,93,93),
        (84646,29,57),
        (10963,95,95),
        (10963,93,98),
        (84646,75,97)]

    dataframe_nota = spark_session.createDataFrame(nota_data,
                                               ['estudiante_id', 'curso_id', 'nota'])

    return dataframe_estudiantes, dataframe_curso, dataframe_nota

# Data set notas sin estudiantes asignados
def data_wrong_1(spark_session):
    estudiantes_data = [
        (99324,'Sandor Gowing','Comunicación'),
        (89598,'Chev Litchmore','Comunicación'),
        (71910,'Puff Van der Merwe','Sociología'),
        (84646,'Lilias Brittan','Sociología'),
        (10963,'Saundra Doble','Periodismo'),
        (63249,'Herrick Penhallurick','Sociología'),
        (99997,'Aubree Broady','Biología'),
        (80802,'Natalina Avrahamy','Comunicación'),
        (99324,'Noland Boyen','Historia'),
        (14780,'Salomo Izzard','Historia')]

    dataframe_estudiantes = spark_session.createDataFrame(estudiantes_data,
                                               ['carnet', 'nombre', 'carrera'])
    curso_data = [
        (93,6,'Filantropía'),
        (29,2,'Biología'),
        (95,9,'Sociología'),
        (75,4,'Historia')]

    dataframe_curso = spark_session.createDataFrame(curso_data,
                                               ['codigo', 'creditos', 'nombre'])
    nota_data = [
        (4585,93,41),
        (77847,29,63),
        (40646,95,93),
        (67170,75,57),
        (16958,52,95),
        (15005,60,98),
        (28949,66,97),
        (6987,93,41),
        (7243,29,53),
        (6987,95,50)]

    dataframe_nota = spark_session.createDataFrame(nota_data,
                                               ['estudiante_id', 'curso_id', 'nota'])

    return dataframe_estudiantes, dataframe_curso, dataframe_nota


# def test_join_dfs_es_no_cur(spark_session):
#     estudiantes, cursos, notas = data_good(spark_session)
    
#     join_df = join_datasets(estudiantes, cursos, notas)

#     expected_ds = spark_session.createDataFrame(
#         [
#             (89598, 'Chev Litchmore', 'Comunicación', 89598, 29, 63, 29, 2, 'Biología'),
#             (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
#             (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, 'Sociología'),
#             (84646, 'Lilias Brittan', 'Sociología', 84646, 75, 97, 75, 4, 'Historia'),
#             (10963, 'Saundra Doble', 'Periodismo', 10963, 93, 98, 93, 6, 'Filantropía'),
#             (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
#             (71910, 'Puff Van der Merwe', 'Sociología', 71910, 93, 93, 93, 6, 'Filantropía')
#         ],
#         ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

#     expected_ds.show()
#     join_df.show()

#     assert join_df.collect() == expected_ds.collect()

# def test_aggregate_datasets(spark_session):
#     estudiantes, cursos, notas = data_good(spark_session)
#     join_df = join_datasets(estudiantes, cursos, notas)

#     aggregate_df = aggregate_datasets(join_df)

#     expected_ds = spark_session.createDataFrame(
#         [
#             (10963, 15, 1443, 96.2),
#             (99324, 6, 246, 41.0),
#             (89598, 2, 126, 63.0),
#             (84646, 6, 502, 83.66666666666667),
#             (71910, 6, 558, 93.0)
#         ],
#         ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'Ponderado'])

#     expected_ds.show()
#     aggregate_df.show()

#     assert aggregate_df.collect() == expected_ds.collect()


def test_join_dfs_es_no_cur_wrong_data(spark_session):
    estudiantes, cursos, notas = data_wrong_1(spark_session)
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        spark_session.sparkContext.emptyRDD()
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

