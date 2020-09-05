from .functions_programa import load_CSV, join_datasets, aggregate_datasets, sort_by_notes

_estudiante1 = (99324,'Sandor Gowing','Comunicación')
_estudiante2 = (89598,'Chev Litchmore','Comunicación')
_estudiante3 = (71910,'Puff Van der Merwe','Sociología')
_estudiante4 = (84646,'Lilias Brittan','Sociología')
_estudiante5 = (10963,'Saundra Doble','Periodismo')

_estudiante6 = (99324,'Sandor Gowing','Comunicación')
_estudiante7 = (89598,'','Comunicación')
_estudiante8 = (71910,'Puff Van der Merwe','')


_curso1 = (93,6,'Filantropía')
_curso2 = (29,2,'Biología')
_curso3 = (95,9,'Sociología')
_curso4 = (75,4,'Historia')

_curso5 = (95,9,'')
_curso6 = (29,2,'Biología')
_curso6 = (95,9,'Sociología')

_nota1 = (99324,93,41)
_nota2 = (89598,29,63)
_nota3 = (71910,93,93)
_nota4 = (84646,29,57)
_nota5 = (10963,95,95)
_nota6 = (10963,93,98)
_nota7 = (84646,75,97)

_nota8 = (84646,75,97)
_nota9 = (84646,75,97)
_nota10 = (84646,75,97)


# Datos sin repetir estudiante
def test_join_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (89598, 'Chev Litchmore', 'Comunicación', 89598, 29, 63, 29, 2, 'Biología'),
            (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, 'Sociología'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (71910, 'Puff Van der Merwe', 'Sociología', 71910, 93, 93, 93, 6, 'Filantropía')
        ],
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

def test_aggregate_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 9, 855, 95.0),
            (99324, 6, 246, 41.0),
            (89598, 2, 126, 63.0),
            (84646, 2, 114, 57.0),
            (71910, 6, 558, 93.0)
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'ponderado'])

    expected_ds.show()
    aggregate_df.show()

    assert aggregate_df.collect() == expected_ds.collect()

def test_sort_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)
    sortable_df = sort_by_notes(aggregate_df, 3)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 9, 855, 95.0),
            (71910, 6, 558, 93.0),
            (89598, 2, 126, 63.0),
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'Ponderado'])

    expected_ds.show()
    sortable_df.show()

    assert sortable_df.collect() == expected_ds.collect()

# Test Estudiantes con varias notas 
def test_join_estudiante_n_notas_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5, _nota6, _nota7]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (89598, 'Chev Litchmore', 'Comunicación', 89598, 29, 63, 29, 2, 'Biología'),
            (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, 'Sociología'),
            (84646, 'Lilias Brittan', 'Sociología', 84646, 75, 97, 75, 4, 'Historia'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 93, 98, 93, 6, 'Filantropía'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (71910, 'Puff Van der Merwe', 'Sociología', 71910, 93, 93, 93, 6, 'Filantropía')
        ],
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

def test_aggregate_estudiante_n_notas_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5, _nota6, _nota7]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 15, 1443, 96.2),
            (99324, 6, 246, 41.0),
            (89598, 2, 126, 63.0),
            (84646, 6, 502, 83.66666666666667),
            (71910, 6, 558, 93.0)
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'ponderado'])

    expected_ds.show()
    aggregate_df.show()

    assert aggregate_df.collect() == expected_ds.collect()

def test_sort_estudiante_n_notas_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5, _nota6, _nota7]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)
    sortable_df = sort_by_notes(aggregate_df, 5)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 15, 1443, 96.2),
            (71910, 6, 558, 93.0),
            (84646, 6, 502, 83.66666666666667),
            (89598, 2, 126, 63.0),
            (99324, 6, 246, 41.0)
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'Ponderado'])

    expected_ds.show()
    sortable_df.show()

    assert sortable_df.collect() == expected_ds.collect()

# Casos sin nombre, carrera
def test_join_estudiante_sin_nombre_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante7, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (89598, '', 'Comunicación', 89598, 29, 63, 29, 2, 'Biología'),
            (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, 'Sociología'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (71910, 'Puff Van der Merwe', 'Sociología', 71910, 93, 93, 93, 6, 'Filantropía')
        ],
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

def test_join_estudiante_sin_carrera_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante8, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (89598, 'Chev Litchmore', 'Comunicación', 89598, 29, 63, 29, 2, 'Biología'),
            (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, 'Sociología'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (71910, 'Puff Van der Merwe', '', 71910, 93, 93, 93, 6, 'Filantropía')
        ],
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

# Curso sin nombre 
def test_join_curso_sin_nombre_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante2, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso5, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (89598, 'Chev Litchmore', 'Comunicación', 89598, 29, 63, 29, 2, 'Biología'),
            (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, ''),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (71910, 'Puff Van der Merwe', 'Sociología', 71910, 93, 93, 93, 6, 'Filantropía')
        ],
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

# Estudiante repetido 
def test_join_estudiante_repetido_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante1, _estudiante1, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, 'Sociología'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía')
        ],
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

def test_aggregate_estudiante_repetido_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante1, _estudiante1, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 9, 855, 95.0),
            (99324, 18, 738, 41.0),
            (84646, 2, 114, 57.0),
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'ponderado'])

    expected_ds.show()
    aggregate_df.show()

    assert aggregate_df.collect() == expected_ds.collect()

def test_sort_estudiante_repetido_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante1, _estudiante1, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)
    sortable_df = sort_by_notes(aggregate_df, 3)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 9, 855, 95.0),
            (84646, 2, 114, 57.0),
            (99324, 18, 738, 41.0),
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'Ponderado'])

    expected_ds.show()
    sortable_df.show()

    assert sortable_df.collect() == expected_ds.collect()

# Test Estudiante repetido con varias notas 
def test_join_estudiante_r_N_notas_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante1, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5, _nota6, _nota7]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])
    
    join_df = join_datasets(estudiantes, cursos, notas)

    expected_ds = spark_session.createDataFrame(
        [
            (84646, 'Lilias Brittan', 'Sociología', 84646, 29, 57, 29, 2, 'Biología'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 95, 95, 95, 9, 'Sociología'),
            (84646, 'Lilias Brittan', 'Sociología', 84646, 75, 97, 75, 4, 'Historia'),
            (10963, 'Saundra Doble', 'Periodismo', 10963, 93, 98, 93, 6, 'Filantropía'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (99324, 'Sandor Gowing', 'Comunicación', 99324, 93, 41, 93, 6, 'Filantropía'),
            (71910, 'Puff Van der Merwe', 'Sociología', 71910, 93, 93, 93, 6, 'Filantropía')
        ],
        ['carnet', 'nombre', 'carrera', 'estudiante_id', 'curso_id', 'nota', 'codigo', 'creditos','nombre'])

    expected_ds.show()
    join_df.show()

    assert join_df.collect() == expected_ds.collect()

def test_aggregate_estudiante_r_N_notas_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante1, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5, _nota6, _nota7]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 15, 1443, 96.2),
            (99324, 12, 492, 41.0),
            (84646, 6, 502, 83.66666666666667),
            (71910, 6, 558, 93.0)
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'ponderado'])

    expected_ds.show()
    aggregate_df.show()

    assert aggregate_df.collect() == expected_ds.collect()

def test_sort_estudiante_r_N_notas_df(spark_session):
    estudiantes_data = [_estudiante1, _estudiante1, _estudiante3, _estudiante4, _estudiante5]
    estudiantes = spark_session.createDataFrame(estudiantes_data,
                                                ['carnet', 'nombre', 'carrera'])

    curso_data = [_curso1, _curso2, _curso3, _curso4]
    cursos = spark_session.createDataFrame(curso_data,
                                            ['codigo', 'creditos', 'nombre'])

    nota_data = [_nota1, _nota2, _nota3, _nota4, _nota5, _nota6, _nota7]
    notas = spark_session.createDataFrame(nota_data,
                                            ['estudiante_id', 'curso_id', 'nota'])

    join_df = join_datasets(estudiantes, cursos, notas)
    aggregate_df = aggregate_datasets(join_df)
    sortable_df = sort_by_notes(aggregate_df, 4)

    expected_ds = spark_session.createDataFrame(
        [
            (10963, 15, 1443, 96.2),
            (71910, 6, 558, 93.0),
            (84646, 6, 502, 83.66666666666667),
            (99324, 12, 492, 41.0)
        ],
        ['carnet', 'sum(creditos)', 'sum(NotaCreditos)', 'Ponderado'])

    expected_ds.show()
    sortable_df.show()

    assert sortable_df.collect() == expected_ds.collect()
