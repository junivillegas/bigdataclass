from .programaestudiante import join_datasets

def good_data(spark_session):
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


def test_join_dfs_es_no_cur(spark_session):
    estudiantes, cursos, notas = good_data(spark_session)
    cursos.show()
    notas.show()
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (6520985, 'Jana Barhems', 'Biotecnologia', 6520985, 3421510, 6.8, 3421510, 2, 'Enfermeria',  13.6),
            (6626704,'Cordelia Shucksmith', 'Enfermeria', 6626704, 4117669, 5.7, 4117669, 8, 'Filologia', 45.6,),
        ],
        ['numeroCarnet', 'nombreCompleto', 'carrera', 'idCarnet', 'codigoCurso', 'nota', 'codigoCurso', 'creditos', 'carrera', 'nota * creditos'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()
