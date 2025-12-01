En esta actividad se integrará Control‑M con Azure Databricks usando el
plugin de Databricks, jobs/notebooks, paso de variables, monitoreo y
comunicación bidireccional. No usamos curl; todo se realiza desde
Control‑M y Azure Databricks. Incluye documentación, explicaciones y
código en Python (requests) para la invocación desde notebooks hacia
Control‑M.

**Objetivo general de la actividad**

-   Orquestar notebooks de Azure Databricks desde Control‑M, pasando
    parámetros, monitoreando ejecuciones y recuperando resultados, y
    habilitar la invocación desde notebooks de Databricks hacia la API
    de Control‑M para señales de estado.

**Prerrequisitos**

-   **Acceso a Azure Databricks:**

    -   Workspace activo (Azure Databricks).

    -   Permisos para crear jobs y ejecutar notebooks.

    -   Token de acceso (PAT) o Service Principal configurado.

-   **Control‑M:**

    -   Acceso a Control‑M Enterprise Manager y Control‑M/Server.

    -   Control‑M/Agent operativo.

    -   Plugin de Databricks disponible (instalable).

    -   Capacidad para crear Connection Profiles y Jobs en el dominio de
        Monitoreo/Planificación.

-   **Entorno de notebooks:**

    -   Python disponible en cluster de Databricks.

    -   Librería requests (incluida en Databricks; si no, instalar via
        %pip install requests).

-   **Buenas prácticas de seguridad:**

    -   Guardar tokens en perfiles seguros o vault (no hardcodear en
        notebooks).

    -   Principio de mínimos privilegios.

Control‑M

**Plugins disponibles**

-   **Identificar el plugin de Databricks:**

    -   **Descripción:** El plugin permite crear y ejecutar jobs de
        Databricks, administrar credenciales y conectar a endpoints del
        workspace desde Control‑M.

    -   **Dónde se usa:** En la definición de jobs dentro de Control‑M,
        aparece un tipo de job “Databricks” (o “Databricks
        Notebooks/Jobs”) con campos específicos.

    -   **Por qué usar plugin:** Reduce la necesidad de scripts custom y
        estandariza autenticación, ejecución y monitoreo dentro del
        ecosistema Control‑M.

    -   **Compatibilidad:** El plugin encapsula llamadas REST de
        Databricks (jobs/runs) y expone campos amigables para
        configurar.

Configuración del plugin

-   **Crear un Connection Profile para Databricks:**

    -   **Workspace URL:** Define la URL del workspace (ej.
        https://&lt;tu-workspace&gt;.azuredatabricks.net).

    -   **Autenticación:** Selecciona “Token” (PAT) o credenciales de
        Service Principal si tu organización usa OAuth.

    -   **Guardar en perfil seguro:** El token/credenciales se almacenan
        sin aparecer en texto plano en la definición del job.

-   **Prueba de conexión:**

    -   **Acción:** Desde Control‑M, realiza un “Test Connection” del
        perfil.

    -   **Validación:** Debe responder como “Success”. Si falla, revisar
        URL/credenciales/permisos.

-   **Explicación adicional:**

    -   **Reutilización:** Un Connection Profile se puede reaprovechar
        en múltiples jobs, unificando credenciales y evitando
        duplicidades.

    -   **Seguridad:** Mantener tokens/secretos en el perfil y no en
        campos visibles de los jobs.

Llamadas a la API REST de Databricks (vía plugin)

-   **Crear un Job Databricks en Control‑M:**

    -   **Tipo de job:** Selecciona “Databricks”.

    -   **Perfil de conexión:** Elige el Connection Profile creado.

    -   **Operación:** Configura “Run job” o “Run notebook”.

    -   **Parámetros básicos:** Job ID (si ya existe en Databricks) o
        ruta del notebook si el plugin soporta ejecución directa de
        notebook tasks.

-   **Ejecución:**

    -   **Acción:** Guardar y ejecutar el job desde Control‑M.

    -   **Resultado esperado:** Control‑M envía la llamada al endpoint
        de Databricks (run-now) y recibe un identificador de ejecución.

    -   **Transparencia:** Aunque el plugin oculta la llamada REST,
        internamente usa endpoints de Jobs API (crear/ejecutar/consultar
        runs).

    -   **Diagnóstico:** Si algo falla, revisar logs del job en
        Control‑M y la UI de Databricks para correlacionar el run.

Paso de variables desde Control‑M a notebooks

-   **Definir variables en Control‑M:**

    -   **Variables de job:** Crea variables (ej. FECHA, REGION,
        CLIENTE) en el job o en nivel de carpeta.

    -   **Mapeo a parámetros:** En el formulario del plugin, mapea
        dichas variables a “notebook parameters”.

-   **Notebook parametrizado:**

    -   **Widgets en Databricks:**

python

dbutils.widgets.text("fecha", "", "Fecha de proceso")

dbutils.widgets.text("region", "", "Región")

dbutils.widgets.text("cliente", "", "Cliente")

fecha = dbutils.widgets.get("fecha")

region = dbutils.widgets.get("region")

cliente = dbutils.widgets.get("cliente")

print(f"Params: fecha={fecha}, region={region}, cliente={cliente}")

-   **Ejecución con parámetros:**

    -   **Acción:** Ejecuta el job en Control‑M con variables definidas.

    -   **Validación:** En el output/log del run en Databricks,
        verificar que se recibieron esos valores.

-   **Explicación adicional:**

    -   **Reutilización:** Un mismo notebook sirve para múltiples
        escenarios con diferentes entradas.

    -   **Encadenamiento:** Control‑M puede calcular variables (ej.
        “fecha de ayer”) y pasarlas sin editar el notebook.

Monitoreo y recuperación de resultados

-   **Monitoreo en Control‑M:**

    -   **Estado del run:** Ver el estado en el “Monitoring domain”
        (Running, Ended OK/Not OK).

    -   **Políticas de reintento:** Configurar reintentos, timeouts y
        alertas ante fallos.

-   **Resultados del proceso:**

    -   **Práctica recomendada:** El notebook escribe resultados de
        negocio en Delta Lake/ADLS/SQL y registra métricas en logs.

    -   **Consumo:** Control‑M puede invocar pasos posteriores (ej.
        mover datos, notificar) en función del estado.

-   **Explicación adicional:**

    -   **Trazabilidad:** Control‑M agrega auditoría sobre cada run,
        mientras Databricks conserva historial técnico (logs/metrics).

    -   **Automatización:** Usa dependencias para encadenar tareas según
        éxito/fallo.

    -   

Azure Databricks

**Lógica de invocación desde notebooks (Databricks → Control‑M)**

-   **Escenario:** El notebook de Databricks envía una señal a Control‑M
    (por ejemplo, “resultado listo” o “error detallado”), llamando a una
    API expuesta por Control‑M.

-   **Seguridad y configuración:**

    -   **Credenciales:** Usa un token/usuario técnico específico para
        la integración con Control‑M (no el mismo de Databricks si no es
        necesario).

    -   **Headers:** Preparar autenticación en headers (Bearer token) y
        Content-Type: application/json.

-   **Ejemplo de código (Python, requests):**

python

import os, requests, json

CONTROL\_M\_URL = os.environ.get("CONTROL\_M\_URL") \# p.ej.
https://controlm.company.com

CONTROL\_M\_TOKEN = os.environ.get("CONTROL\_M\_TOKEN")

headers = {

"Authorization": f"Bearer {CONTROL\_M\_TOKEN}",

"Content-Type": "application/json"

}

payload = {

"notebook\_run\_id":
dbutils.notebook.entry\_point.getDbutils().notebook().getContext().currentRunId().get(),
\# opcional

"status": "SUCCESS",

"message": "Proceso finalizado y resultados disponibles",

"metrics": {"rows": 123456, "duration\_sec": 87}

}

resp =
requests.post(f"{CONTROL\_M\_URL}/api/integrations/databricks/notify",
\# ejemplo de endpoint expuesto

headers=headers, data=json.dumps(payload), timeout=30)

if not resp.ok:

print("Notificación a Control-M falló:", resp.status\_code, resp.text)

else:

print("Notificación enviada a Control-M")

-   **Bidireccionalidad:** Esta técnica permite que Databricks no solo
    reciba órdenes, sino que reporte resultados/estados en tiempo real a
    Control‑M.

-   **Desacoplamiento:** Si Control‑M no expone API directa, puedes usar
    intermediarios (Service Bus, Event Grid, tabla SQL, archivo “flag”
    en storage) que Control‑M monitorea.

Uso de requests para llamar a la API de Control‑M

-   **Documentación interna:**

    -   **Librería requests:** Simplifica HTTP (GET/POST) y manejo de
        JSON.

    -   **Manejo de errores:** Validar resp.ok, timeouts, reintentos.

-   **Patrones comunes:**

    -   **Notificar estado:** POST con status, message, metrics.

    -   **Consultar:** GET de “job state” si existe un endpoint de
        consulta.

-   **Ejemplo de consulta (si Control‑M expone un estado de job):**

python

status\_resp =
requests.get(f"{CONTROL\_M\_URL}/api/integrations/jobs/state?jobKey=ETL\_DAILY",

headers=headers, timeout=30)

if status\_resp.ok:

print("Estado job:", status\_resp.json())

-   **Explicación adicional:**

    -   **Idempotencia:** Si el notebook puede reintentar, asegúrate de
        que las notificaciones no generen duplicados (usa
        correlación/run\_id).

    -   **Seguridad:** No logs con tokens; usar variables de
        entorno/secret scopes.

Autenticación y headers

-   **Databricks (Plugin en Control‑M):**

    -   **Header de autorización:** Se gestiona dentro del Connection
        Profile (Bearer &lt;token&gt;).

    -   **Content-Type:** JSON para payloads (configurado por el
        plugin).

-   **Notebooks → Control‑M:**

    -   **Headers mínimos:**

        -   Authorization: Bearer &lt;TOKEN&gt;

        -   Content-Type: application/json

    -   **Gestión de tokens:**

        -   Variables de entorno o Secret Scopes (Databricks).

        -   Rotación periódica y mínimos privilegios.

    -   **Consistencia:** Define funciones auxiliares para construir
        headers, reducir errores y facilitar mantenimiento.

    -   **Auditoría:** Control‑M registra quién ejecuta y desde dónde;
        mantén tokens dedicados para trazabilidad.

**Verificación integral**

-   **Ejecución end‑to‑end:**

    -   Desde Control‑M, lanza el job de Databricks con parámetros.

    -   Observa el estado en Control‑M y verifica logs en Databricks.

    -   Comprueba que el notebook escribe resultados (Delta/ADLS/SQL).

    -   Opcional: el notebook notifica a Control‑M vía API (requests).

-   **Validaciones clave:**

    -   **Parámetros:** Los widgets reciben las variables enviadas por
        Control‑M.

    -   **Estados:** Control‑M refleja SUCCESS/FAILED coherente con el
        run.

    -   **Seguridad:** Tokens guardados en perfiles/secret scopes; sin
        exposición en código.

-   **Posibles problemas:**

    -   **Conexión fallida:** Revisar URL/credenciales/permisos en
        Connection Profile.

    -   **Parámetros no recibidos:** Validar nombres y mapeo entre
        variables de Control‑M y dbutils.widgets.

    -   **Monitoreo inconsistente:** Ajustar timeouts/reintentos en
        Control‑M; revisar colas/cluster en Databricks.

    -   **Notificación desde notebook fallida:** Verificar endpoint de
        Control‑M, tokens y firewall.
