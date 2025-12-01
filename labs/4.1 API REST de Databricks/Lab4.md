Este laboratorio se realizará el proceso de autenticación con tokens,
llamada a endpoints clave, ejecución de notebooks vía API, paso de
variables y monitoreo de ejecuciones.

Databricks recomienda usar OAuth con Service Principals para procesos no
interactivos; las herramientas oficiales pueden intercambiar el token
federado por un token Databricks y renovarlo automáticamente, evitando
manejar PAT manualmente.

bash

\# Variables de entorno (ejemplo bash)

export
DATABRICKS\_URL="https://&lt;tu-workspace&gt;.azuredatabricks.net"

export DATABRICKS\_TOKEN="dapiXXXXXXXX..." \# usa un vault si es
producción

Autenticación con tokens

Realizar una llamada autenticada a la API REST de Databricks usando el
header Authorization: Bearer.

**Acciones**

-   **Genera un token** (PAT) desde User Settings si estás en
    desarrollo, o usa **OAuth con Service Principal** si estás en
    producción.

-   **Guárdalo** como secreto/variable de entorno y **no lo expongas**
    en logs.

**Ejemplo curl (probar autenticación listando jobs)**

bash

curl -s -X GET "$DATABRICKS\_URL/api/2.0/jobs/list" \\

-H "Authorization: Bearer $DATABRICKS\_TOKEN"

**Ejemplo Python (requests)**

python

import os, requests

url = os.environ\["DATABRICKS\_URL"\]

token = os.environ\["DATABRICKS\_TOKEN"\]

headers = {"Authorization": f"Bearer {token}"}

r = requests.get(f"{url}/api/2.0/jobs/list", headers=headers,
timeout=30)

print(r.status\_code, r.json() if r.ok else r.text)

**Validación**

-   **200 OK:** Autenticación correcta y respuesta JSON con jobs.

-   **401/403:** Revisa token, permisos o vencimiento.

Los componentes típicos de una llamada REST incluyen la URL del
workspace, el método (GET/POST), la ruta del endpoint y la autenticación
con token; Databricks documenta estos aspectos en su referencia de API y
recomienda OAuth para procesos desatendidos.

Endpoints principales (jobs, runs, notebooks, clusters)

Conocer y probar los endpoints REST más usados para orquestación.

**Acciones**

-   **Jobs:** crear, listar y ejecutar definiciones de trabajo.

-   **Runs:** consultar ejecuciones en curso e históricas.

-   **Clusters:** administrar clusters (crear, iniciar, terminar).

-   **Notebooks:** gestionar notebooks (ruta, tareas dentro de jobs).

**Ejemplo curl (clusters list)**

bash

curl -s -X GET "$DATABRICKS\_URL/api/2.0/clusters/list" \\

-H "Authorization: Bearer $DATABRICKS\_TOKEN"

**Ejemplo Python (jobs list)**

python

r = requests.get(f"{url}/api/2.0/jobs/list", headers=headers,
timeout=30)

for job in r.json().get("jobs", \[\]):

print(job\["job\_id"\], job\["settings"\]\["name"\])

**Validación**

-   Ver entradas de clusters/jobs; si no hay, crearás uno en el
    siguiente paso.

La referencia oficial cubre Jobs y Clusters APIs; entender estas rutas
es clave para integrarse con orquestadores y realizar operaciones como
run-now y runs/get para monitoreo.

Ejecución de notebooks vía API

Definir un job con un notebook\_task y ejecutarlo programáticamente con
run-now.

**Acciones**

1.  **Crear un job** con un notebook\_task (una sola vez).

2.  **Ejecutar el job** con run-now.

3.  **Guardar el** run\_id para monitorear el estado.

**Crear job (curl)**

bash

curl -s -X POST "$DATABRICKS\_URL/api/2.1/jobs/create" \\

-H "Authorization: Bearer $DATABRICKS\_TOKEN" \\

-H "Content-Type: application/json" \\

-d '{

"name": "Job\_Ejemplo\_Notebook",

"tasks": \[{

"task\_key": "t1",

"notebook\_task": {"notebook\_path": "/Users/miguel/demo\_notebook"},

"job\_cluster\_key": "jobc1"

}\],

"job\_clusters": \[{

"job\_cluster\_key": "jobc1",

"new\_cluster": {"spark\_version": "13.3.x-scala2.12", "num\_workers":
2}

}\]

}'

**Ejecutar job (curl)**

bash

curl -s -X POST "$DATABRICKS\_URL/api/2.0/jobs/run-now" \\

-H "Authorization: Bearer $DATABRICKS\_TOKEN" \\

-H "Content-Type: application/json" \\

-d '{"job\_id": 12345}'

\# Respuesta: {"run\_id": 67890}

**Ejecutar job (Python) y capturar run\_id**

python

payload = {"job\_id": 12345}

r = requests.post(f"{url}/api/2.0/jobs/run-now", headers=headers,
json=payload, timeout=30)

run\_id = r.json()\["run\_id"\]

print("Run ID:", run\_id)

**Validación**

-   Debes recibir run\_id; el job aparecerá como “Pending/Running” en la
    UI.

La ejecución de notebooks vía API se realiza a través de la Jobs API
(create, run-now y runs/get). Es la vía recomendada para disparos ad hoc
o orquestación desde sistemas externos como Control‑M.

Paso de variables

Parametrizar notebooks para recibir valores dinámicos desde la API (y
Control‑M).

**Acciones**

-   **Enviar parámetros** en notebook\_params al invocar run-now.

-   **Definir widgets** en el notebook para leerlos.

**Notebook (Python) — widgets**

python

dbutils.widgets.text("fecha", "2025-12-01", "Fecha de proceso")

dbutils.widgets.text("region", "CDMX", "Región")

fecha = dbutils.widgets.get("fecha")

region = dbutils.widgets.get("region")

print(f"Procesando fecha={fecha}, region={region}")

**Invocar con parámetros (curl)**

bash

curl -s -X POST "$DATABRICKS\_URL/api/2.0/jobs/run-now" \\

-H "Authorization: Bearer $DATABRICKS\_TOKEN" \\

-H "Content-Type: application/json" \\

-d '{

"job\_id": 12345,

"notebook\_params": {

"fecha": "2025-12-02",

"region": "NORTE"

}

}'

**Invocar con parámetros (Python)**

python

payload = {

"job\_id": 12345,

"notebook\_params": {"fecha": "2025-12-02", "region": "NORTE"}

}

r = requests.post(f"{url}/api/2.0/jobs/run-now", headers=headers,
json=payload, timeout=30)

print(r.json())

**Validación**

-   Ver en logs del run que el notebook recibió los valores y los usó en
    el procesamiento.

**Contexto y documentación**

Los parámetros fluyen desde workflows/jobs a notebooks mediante
notebook\_params y se leen con widgets. Esto habilita ejecución dinámica
en SQL y Python notebooks y se documenta en la comunidad y KB de
Databricks.

**Paso 5: Monitoreo**

**Objetivo**

Consultar el estado del run, capturar resultados y manejar errores desde
fuera de la UI.

**Acciones**

-   **Consultar estado** con runs/get usando el run\_id.

-   **Poll** hasta que el life\_cycle\_state sea TERMINATED.

-   **Validar** result\_state (SUCCESS o FAILED) y leer mensajes.

**Monitoreo (Python — polling simple)**

python

import time

def wait\_for\_run(run\_id, poll\_sec=10, timeout\_sec=1800):

start = time.time()

while True:

r = requests.get(f"{url}/api/2.1/jobs/runs/get?run\_id={run\_id}",
headers=headers, timeout=30)

js = r.json()

state = js\["state"\]\["life\_cycle\_state"\]

result = js\["state"\].get("result\_state")

print(f"Run {run\_id}: state={state}, result={result}")

if state == "TERMINATED":

return js

if time.time() - start &gt; timeout\_sec:

raise TimeoutError(f"Run {run\_id} timeout")

time.sleep(poll\_sec)

run\_json = wait\_for\_run(run\_id)

if run\_json\["state"\].get("result\_state") != "SUCCESS":

raise RuntimeError(f"Run failed: {run\_json\['state'\]}")

**Monitoreo (curl — estado puntual)**

bash

curl -s -X GET "$DATABRICKS\_URL/api/2.1/jobs/runs/get?run\_id=67890" \\

-H "Authorization: Bearer $DATABRICKS\_TOKEN"

**Recuperación de resultados**

-   **Logs y output:** usa la UI/CLI, o la API para metadatos del run;
    para datos de negocio, escribe resultados a Delta/ADLS y consúmelos
    desde el siguiente paso del pipeline.

-   **Errores:** captura error\_trace y state.message para diagnosticar
    y decidir reintentos.

**Contexto y documentación**

El monitoreo programático se basa en Jobs API (runs/get) y prácticas
comunes de polling; existen scripts de ejemplo para extraer job runs y
logs vía REST que puedes adaptar a tu entorno.

**Notas de solución de problemas**

-   **401/403 al llamar la API:** valida el token, su vigencia y
    permisos del principal/usuario.

-   **Cluster no disponible:** crea un job cluster dentro del job o usa
    un all‑purpose cluster existente; revisa Clusters API para estados y
    acciones.

-   **Notebook no recibe parámetros:** confirma claves en
    notebook\_params y nombres de widgets; sincroniza configuración del
    job con el notebook.

-   **Runs estancados:** ajusta timeouts, revisa colas de cluster y
    cuota; usa polling con backoff y alertas en tu orquestador.
