Los trabajos Lakeflow de Azure Databricks proporcionan una plataforma
robusta para desplegar cargas de trabajo de forma eficiente. Con
funciones como Azure Databricks Jobs y Delta Live Tables, los usuarios
pueden orquestar procesos complejos de procesamiento de datos,
aprendizaje automático y procesos de análisis.

<span class="mark">**Nota:** La interfaz de usuario de Azure Databricks
está sujeta a mejoras continuas. La interfaz de usuario puede haber
cambiado.</span>

Conectar al workspace creado previamente

1.  Iniciar sesión en Azure.

2.  Entrar al servicio de Azure Databricks creado previamente

3.  Seleccionar **Launch Workspace**

Crea un notebook

1.  En la barra lateral, usa el **enlace (+) New** para crear un
    **Notebook**.

2.  Cambia el nombre predeterminado del cuaderno a **Tarea ETL** y, en
    la lista desplegable **Connect,** selecciona
    **Serverless** **compute** si aún no está seleccionada. Si el
    clúster no se está ejecutando, puede tardar un minuto
    aproximadamente en iniciarse.

Datos de ingesta

1.  En la primera celda del cuaderno, introduce el siguiente código para
    crear un volumen que almacene algunos archivos de laboratorio.

**%sql**

**CREATE VOLUME IF NOT EXISTS spark\_lab**

1.  Añade una nueva celda de código y úsala para ejecutar el siguiente
    código, que utiliza *Python* para descargar archivos de datos desde
    GitHub a tu volumen.

> **import requests**
>
> **\# Defina el catálogo actual**
>
> **catalog\_name = spark.sql("SELECT
> current\_catalog()").collect()\[0\]\[0\]**
>
> **\# Define la ruta base usando el catálogo actual**
>
> **volume\_base = f"/Volumes/{catalog\_name}/default/spark\_lab"**
>
> **\# Lista de archivos para descargar**
>
> **files = \["2019.csv", "2020.csv", "2021.csv"\]**
>
> **\# Descarga cada archivo**
>
> **for file in files:**
>
> **url =
> f"https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/{file}"**
>
> **response = requests.get(url)**
>
> **response.raise\_for\_status()**
>
> **\# Volumen Write to Unity Catalog**
>
> **with open(f"{volume\_base}/{file}", "wb") as f:**
>
> **f.write(response.content)**

1.  Usa la opción de menú **Run cell** a la izquierda de la celda para
    ejecutarlo. Luego espera a que el trabajo de Spark ejecutado por el
    código se complete.

2.  Bajo la salida, utiliza el icono **+ Code** para añadir una nueva
    celda de código y úsalo para ejecutar el siguiente código, que
    define un esquema para los datos:

**from pyspark.sql.types import \***

**from pyspark.sql.functions import \***

**orderSchema = StructType(\[**

**StructField("SalesOrderNumber", StringType()),**

**StructField("SalesOrderLineNumber", IntegerType()),**

**StructField("OrderDate", DateType()),**

**StructField("CustomerName", StringType()),**

**StructField("Email", StringType()),**

**StructField("Item", StringType()),**

**StructField("Quantity", IntegerType()),**

**StructField("UnitPrice", FloatType()),**

**StructField("Tax", FloatType())**

**\])**

**df =
spark.read.load(f'/Volumes/{catalog\_name}/default/spark\_lab/\*.csv',
format='csv', schema=orderSchema)**

**display(df.limit(100))**

Crear un job task

1.  Bajo la celda de código existente, utiliza el icono **+Code** para
    añadir una nueva celda de código. Luego, en la nueva celda,
    introduce y ejecuta el siguiente código para eliminar filas
    duplicadas y reemplazar las entradas nulas por los valores
    correctos:

**from pyspark.sql.functions import col**

**df = df.dropDuplicates()**

**df = df.withColumn('Tax', col('UnitPrice') \* 0.08)**

**df = df.withColumn('Tax', col('Tax').cast("float"))**

**Nota:** Tras actualizar los valores en la columna **Tax**, su tipo de
dato vuelve a flotar. Esto se debe a que su tipo de dato cambia a
duplicarse tras realizar el cálculo. Dado que double tiene un uso de
memoria mayor que float, es mejor para el rendimiento hacer cast de la
columna de vuelta a flotante.

1.  En una nueva celda de código, ejecuta el siguiente código para
    agregar y agrupar los datos de orden:

**yearlySales =
df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")**

**display(yearlySales)**

Construye el flujo de trabajo

Azure Databricks gestiona la orquestación de tareas, la gestión de
clústeres, la monitorización y el reporte de errores para todos tus
trabajos. Puedes ejecutar tus trabajos de forma inmediata,
periódicamente a través de un sistema de planificación fácil de usar,
cada vez que lleguen nuevos archivos a una ubicación externa, o de forma
continua para asegurarte de que una instancia del trabajo esté siempre
en ejecución.

1.  En tu espacio de trabajo, haz clic en **Jobs & Pipelines**  en la
    barra lateral.

2.  En el panel de Trabajos y Canalizaciones, selecciona **Create** y
    luego **Job**.

3.  Cambia el nombre predeterminado del trabajo a **job ETL**.

4.  Configura el trabajo con los siguientes ajustes:

    -   **Task name**: Ejecutar cuaderno de tareas ETL

    -   **Type**: Cuaderno

    -   **Source**: Workspace

    -   **Path**: *Selecciona tu* cuaderno *de tareas ETL*

    -   **Cluster:** *Seleccionar Serverless*

5.  Selecciona **Create task**.

6.  Selecciona **Run now**.

7.  Cuando el job empieza a ejecutarse, puedes monitorear su ejecución
    seleccionando **Job Runs** en la barra lateral izquierda.

8.  Después de que la ejecución del trabajo tenga éxito, puedes
    seleccionarlo y verificar su salida.

Además, puedes ejecutar jobs de forma desencadenada, por ejemplo,
ejecutando un flujo de trabajo con un calendario. Para programar una
ejecución periódica de trabajos, puedes abrir la tarea y añadir un
trigger.
