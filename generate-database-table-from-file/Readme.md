# README

## Descripción
Este script crea una tabla en una base de datos PostgreSQL a partir de la estructura de un archivo CSV o Excel. Permite cargar datos automáticamente en la tabla generada, infiriendo los tipos de datos de las columnas del archivo y configurando las columnas adecuadamente en PostgreSQL.

## Prerrequisitos
Antes de ejecutar el script, asegúrate de cumplir con los siguientes requisitos:

1. Actualiza tu sistema e instala las dependencias necesarias ejecutando:
   ```bash
   sudo apt update
   sudo apt install python3.12-dev
   sudo apt install libpq-dev
   ```
2. Instala las bibliotecas de Python necesarias:
   ```bash
   pip install -r requirements.txt
   ```
3. Configura las variables de entorno necesarias para la conexión a PostgreSQL:
   ```bash
   export DB_USER="{tu_usuario}"
   export DB_PASSWORD="{tu_contraseña}"
   export DB_HOST="{host}"                  # por ejemplo: localhost
   export DB_PORT="{port}"                  # por ejemplo: 5432
   ```

## Uso
El script acepta varios argumentos de línea de comandos para personalizar su ejecución. Aquí se describen los pasos para ejecutarlo:

### Sintaxis general
```bash
python3 generate-database-table-from-file.py -f <ruta_al_archivo> [-s <esquema>] [-d <base_de_datos>] [-t <nombre_tabla>] [-m <modo>] [--show-sql]
```

### Argumentos

- `-f`, `--file`: **(Requerido)** Ruta al archivo CSV o Excel que contiene los datos.
- `-s`, `--schema`: Esquema de la base de datos donde se creará la tabla. Por defecto, se usa `public`.
- `-d`, `--database`: Nombre de la base de datos. Por defecto, se usa `python_scripts`.
- `-t`, `--table`: Nombre de la tabla a crear. Si no se especifica, el nombre se genera a partir del nombre del archivo.
- `-m`, `--mode`: Modo de operación:
  - `replace`: Elimina todos los datos existentes en la tabla antes de cargar los nuevos.
  - `update` (por defecto): Añade o actualiza los datos en la tabla.
- `--show-sql`: Muestra las consultas SQL ejecutadas durante el proceso.

### Ejemplos de uso

#### Crear una tabla con el nombre derivado del archivo
```bash
python3 generate-database-table-from-file.py -f datos.csv
```

#### Crear una tabla en un esquema específico
```bash
python3 generate-database-table-from-file.py -f datos.xlsx -s analytics
```

#### Crear una tabla con un nombre personalizado
```bash
python3 generate-database-table-from-file.py -f datos.csv -t ventas
```

#### Reemplazar los datos existentes en la tabla
```bash
python3 generate-database-table-from-file.py -f datos.xlsx -m replace
```

#### Mostrar las consultas SQL ejecutadas
```bash
python3 generate-database-table-from-file.py -f datos.csv --show-sql
```

## Resultado
1. Una vez ejecutado el script, se creará una tabla en la base de datos especificada.
2. Los datos del archivo serán cargados en la tabla.
3. Para verificar el resultado, conecta a la base de datos y consulta la tabla creada con una herramienta como `psql` o un cliente gráfico como DBeaver.

```sql
SELECT * FROM <esquema>.<nombre_tabla>;
```

## Registro de logs
El script genera registros detallados durante su ejecución. Estos registros incluyen:
- Información sobre la lectura del archivo.
- Tipos de columnas inferidos.
- Consultas SQL ejecutadas (si se habilita `--show-sql`).
- Mensajes de éxito o errores en el proceso.

