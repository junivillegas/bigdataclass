# FUNDATEC

**Estudiante: Jaime Villegas Gallardo**

**Curso: Big Data**

**Profesor: Juan Manuel Esquivel Rodriguez**

## Tarea 2

#### Supuestos
El metodo del sort que se utilizo en la tarea 1 y se indico como incorrecto se utilizo en este ejercicio dado a que no se obtuvo respuesta sobre si estaba incorrecto o era otro error.

Para la elaboracion del programa principal se tomara como base el docker container proporcionado para el curso y se creara una carpeta con el nombre tarea2 donde se ubicaran los archivos a evaluar.

Deacuerdo al enunciado de la tarea al ejecutar programaestudiante.sh se cargaran todos los archivos en cajas/*.json

### Instrucciones
Descomprimir el archivo tarea2.zip y navegar al contenido de la carpeta 'tarea2'

Construir el contenedor y copiar los archivos
```sh
$ ./build_image.sh
```
Ejecutar el contenedor 
```sh
$ ./run_image.sh
```
Acceder a la carpeta del trabajo
```sh
$ cd tarea2/
```

#### Programa principal 
El programa principal se encuentra compuesto por: 
- functions_programa.py - funciones para manipular la data
- programaestudiante.py - llamado a las funciones

Ejecutar programa principal
```sh
$ ./programaestudiante.sh
```

#### Pruebas
Las pruebas se encuentran en el archivo test_programaestudiante.py

Ejecutar pruebas
```sh
$ pytest
```