\# AppViewLite Health Monitoring



AppViewLite provides a Prometheus-compatible health metrics endpoint at `/\_health`. This endpoint exposes operational and diagnostic information from AppViewLite, allowing administrators to monitor their instances using \[Prometheus](https://prometheus.io/) and \[Grafana](https://grafana.com/).



The endpoint consolidates information from several existing debug pages:



\- `/debug/counters`

\- `/debug/event-charts`

\- `/debug/locks`

\- `/debug/performance`

\- `/debug/requests`

\- `/debug/tables`



Metrics are exposed in Prometheus text exposition format and can be scraped directly without requiring an additional exporter.



\## Configuration



Access to the health endpoint is disabled by default. To enable it, configure the `APPVIEWLITE\_HEALTH\_IP` environment variable with a comma-separated list of IP addresses authorized to access the endpoint.



Example:



```bash

APPVIEWLITE\_HEALTH\_IP=127.0.0.1,192.168.1.50

```



Multiple addresses can be specified. Commas, semicolons, spaces, and newlines are accepted as separators. Both IPv4 and IPv6 addresses are supported.



The configuration is read when AppViewLite starts. Restart AppViewLite after changing the environment variable.



\### Access control behaviour



| Condition | Response |

|---|---|

| IP is authorized | HTTP 200 with Prometheus metrics |

| IP is not authorized | HTTP 403 Forbidden |

| Environment variable is unset or empty | HTTP 404 Not Found |

| Environment variable contains invalid addresses | Invalid entries are ignored |



IPv4-mapped IPv6 addresses are normalized before comparison.



\*\*Important:\*\* Access control uses the remote IP address reported by the server connection. If AppViewLite is behind a reverse proxy, configure the allowlist for the address AppViewLite actually sees. Do not assume that the original client IP will automatically be used.



\## Prometheus configuration



Add AppViewLite as a scrape target in your Prometheus configuration (`prometheus.yml`).



\### Example



```yaml

scrape\_configs:

&#x20; - job\_name: appviewlite

&#x20;   scrape\_interval: 15s

&#x20;   metrics\_path: /\_health

&#x20;   static\_configs:

&#x20;     - targets:

&#x20;         - 192.168.1.100:61749

```



Replace the target address and port with the address and listening port of your AppViewLite instance.



If AppViewLite uses HTTPS, configure the scrape job accordingly:



```yaml

scrape\_configs:

&#x20; - job\_name: appviewlite

&#x20;   scrape\_interval: 15s

&#x20;   metrics\_path: /\_health

&#x20;   scheme: https

&#x20;   static\_configs:

&#x20;     - targets:

&#x20;         - 192.168.1.100:61749

```



If the server uses a certificate issued by a private certificate authority, configure Prometheus to trust that CA:



```yaml

&#x20;   tls\_config:

&#x20;     ca\_file: /etc/prometheus/certs/appviewlite-ca.crt

```



For testing on a trusted private network, certificate verification can alternatively be disabled:



```yaml

&#x20;   tls\_config:

&#x20;     insecure\_skip\_verify: true

```



Disabling certificate verification is not recommended for production environments.



After updating the configuration, reload or restart Prometheus.



\### Verify scraping



Open the Prometheus web interface and navigate to:



`Status → Targets`



Find the `appviewlite` scrape job. Its state should be `UP`.



You can also query individual metrics from the Prometheus expression browser.



For example:



```promql

appviewlite\_build\_info

```



This reports the AppViewLite build version.



\## Available metrics



All exported metrics use the `appviewlite\_` prefix.



\### Application counters



Numeric values from the existing debug counters are exported through:



```promql

appviewlite\_debug\_counter

```



The original counter name is preserved in the `name` label.



Example:



```promql

appviewlite\_debug\_counter{name="SomeCounter"}

```



Nested numeric values are flattened into dot-separated names. Non-numeric values and arrays are omitted.



\### Firehose



Firehose metrics provide visibility into event processing and ingestion.



Examples:



```promql

appviewlite\_firehose\_received\_events\_total

appviewlite\_firehose\_processing\_lag\_seconds

appviewlite\_firehose\_restart\_count

appviewlite\_firehose\_last\_processed\_timestamp\_seconds

appviewlite\_firehose\_event\_timestamp\_seconds

```



Historical event chart data is also exposed through:



```promql

appviewlite\_firehose\_events\_received\_per\_second

appviewlite\_firehose\_events\_processed\_per\_second

appviewlite\_firehose\_processing\_lag\_behind

```



The historical chart metrics contain up to 120 buckets, identified by `bucket` and `bucket\_seconds` labels.



\### Locks



Lock metrics expose current reader activity and waiting threads for the primary database and read-only replica, where available.



```promql

appviewlite\_lock\_current\_readers

appviewlite\_lock\_waiting\_readers

appviewlite\_lock\_waiting\_writers

appviewlite\_lock\_waiting\_upgraders

appviewlite\_lock\_recursive\_readers

```



Use these metrics to identify lock contention and potential bottlenecks.



\### Performance



Storage operation metrics expose operation counts, durations, IO activity, seeks, and memory allocation.



```promql

appviewlite\_operation\_count\_total

appviewlite\_operation\_average\_duration\_seconds

appviewlite\_operation\_duration\_seconds\_total

appviewlite\_operation\_seek\_duration\_seconds\_total

appviewlite\_operation\_io\_reads\_total

appviewlite\_operation\_io\_read\_bytes\_total

appviewlite\_operation\_mmap\_potential\_read\_bytes\_total

appviewlite\_operation\_seek\_count\_total

appviewlite\_operation\_allocated\_bytes\_total

```



Metrics include labels identifying the operation, table, and context.



For example, to see storage operation throughput:



```promql

sum by (table, operation) (

&#x20; rate(appviewlite\_operation\_count\_total\[5m])

)

```



\### Recent requests



Recent request diagnostics are exported as individual gauge entries.



```promql

appviewlite\_recent\_request\_age\_seconds

appviewlite\_recent\_request\_reads\_secondary

appviewlite\_recent\_request\_reads\_primary

appviewlite\_recent\_request\_writes\_primary

appviewlite\_recent\_request\_write\_or\_upgrade\_locks

appviewlite\_recent\_request\_lock\_wait\_seconds

appviewlite\_recent\_request\_primary\_lock\_seconds

appviewlite\_recent\_request\_secondary\_lock\_seconds

appviewlite\_recent\_request\_max\_gc\_generation\_inside\_lock

```



Request metrics include bounded labels:



\- `urgency`: urgent or nonurgent.

\- `request\_slot`: position within the retained request list.

\- `path`: normalized request category.



Recent request entries are sampled from the existing request queues. Idle requests and the `/debug/requests` endpoint itself are excluded.



These metrics are useful for identifying requests experiencing excessive database lock waits or slow database operations.



\### Database tables



Table metrics expose disk usage, buffered memory, and storage statistics.



```promql

appviewlite\_table\_disk\_bytes

appviewlite\_table\_buffered\_bytes

appviewlite\_table\_page\_index\_bytes

appviewlite\_table\_keys

appviewlite\_table\_values

appviewlite\_table\_average\_values\_per\_key

appviewlite\_table\_slices

appviewlite\_table\_original\_write\_bytes

appviewlite\_table\_compaction\_write\_bytes

appviewlite\_table\_last\_flushed\_timestamp\_seconds

```



Metrics include `table` and `behavior` labels.



For example, to display the largest tables:



```promql

topk(10, appviewlite\_table\_disk\_bytes)

```



\## Grafana configuration



Once Prometheus is successfully scraping AppViewLite, configure Grafana to use it as a data source.



1\. Open Grafana.

2\. Navigate to \*\*Connections → Data sources\*\*.

3\. Select \*\*Add data source\*\*.

4\. Choose \*\*Prometheus\*\*.

5\. Enter the URL of your Prometheus server.

6\. Select \*\*Save \& test\*\*.



\### Importing the example dashboard



An example dashboard is included in the AppViewLite repository:



`docs/appviewlite-grafana-dashboard.json`



To import it:



1\. Open Grafana.

2\. Navigate to \*\*Dashboards → New → Import\*\*.

3\. Upload `appviewlite-grafana-dashboard.json`.

4\. Select your Prometheus data source when prompted.

5\. Select \*\*Import\*\*.



The dashboard provides an overview of AppViewLite, including firehose activity, database locks, storage performance, table statistics, and recent request diagnostics.



The dashboard can be customized to suit individual deployments.



\## Troubleshooting



\### Prometheus reports HTTP 403



The Prometheus server's source IP is not included in `APPVIEWLITE\_HEALTH\_IP`.



Check the IP address AppViewLite sees for incoming connections and add it to the allowlist.



Restart AppViewLite after changing the environment variable.



\### Prometheus reports HTTP 404



The health endpoint is disabled when `APPVIEWLITE\_HEALTH\_IP` is unset or empty.



Configure the environment variable and restart AppViewLite.



\### TLS certificate verification errors



If Prometheus connects using an IP address that is not included in the certificate's Subject Alternative Names (SANs), certificate verification will fail.



Use a certificate valid for the target hostname or IP address, or configure Prometheus to trust the issuing CA.



Disabling certificate verification is possible for testing on trusted networks, but is not recommended for production.



\### Prometheus target is DOWN



Check the following:



\- AppViewLite is running.

\- The health endpoint is enabled.

\- Prometheus can reach the configured host and port.

\- The Prometheus server IP is authorized.

\- The configured HTTP or HTTPS scheme matches the AppViewLite listener.

\- TLS certificate validation succeeds, if HTTPS is enabled.



\## Security considerations



The health endpoint exposes internal operational information, including database statistics, request activity, and performance measurements.



It should only be accessible to trusted monitoring infrastructure.



Use the IP allowlist, restrict network access through firewall rules, and use properly configured TLS when exposing the endpoint across network boundaries.

