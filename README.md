# Kafka Gen Connector
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](../../LICENSE.md)&nbsp;
[![en](https://img.shields.io/badge/lang-fr-red.svg)](README-fr.md)

The Generic Resource performs a check if the expected message is already present on a Kafka topic and immediately returns a success/failure.

# Disclaimer
No support or warranties will be provided by Absyss SAS for this project and associated files. Use is at your own risk.

Absyss SAS cannot be held responsible for damages caused by the use of any files made available in this Github repository.

Consulting days can be requested for implementation.

# Prerequisites

  * Visual TOM 7.3.2 or higher
  * Python 3.x or higher
  * Access to a Kafka cluster / topic
  * Install required python packages with pip: `pip install -r requirements.txt`

# Instructions
  * Windows :
    * Place the file `kafkaGen.py` in the `%ABM_BIN%` directory (or your scripts directory)
    * Declare the queue / custom application on the agent (see administrator guide)

  * Linux/Unix :
    * Place the file `kafkaGen.py` in the `$TOM_ADMIN` directory (or your scripts directory)
    * Declare the queue / custom application on the agent (see administrator guide)

  * Create a "Generic Resource" in Visual TOM with the following definition or import the KafkaGen.xml file:
   ```
   vtimport -x -f KafkaGen.xml
   ```
   ![Custom application screenshot](screenshots/kafkaGen.png?raw=true)

The script returns specific exit codes:
- `0`: Matching message found
- `1`: No matching message / error

Parameter descriptions:
  * `Param 1` BOOTSTRAP_SERVERS : Kafka bootstrap servers (required), e.g. `localhost:9092`
  * `Param 2` TOPIC : Kafka topic name (required)
  * `Param 3` GROUP_ID : Consumer group id (required, created automatically)
  * `Param 4` EXPECTED_TEXT : Expected string in the message (required)
  * `Param 5` COMMIT_MODE : Optional commit mode (default `0`)
    * `0` : no commit
    * `1` : commit the fetched batch
    * `2` : commit only up to the first matching message
  * `Param 6` EXPECTED_IDS : Optional comma-separated IDs expected in the message
  * `Param 7` SECURITY_PROTOCOL : Optional (`PLAINTEXT` / `SASL_SSL` / `SSL`, default `PLAINTEXT`)
  * `Param 8` SASL_MECHANISM : Optional SASL mechanism (e.g. `PLAIN`)
  * `Param 9` USERNAME : Optional SASL username
  * `Param 10` PASSWORD : Optional SASL password
  * `Param 11` SSL_CAFILE : Optional CA certificate path

Protect parameters with double quotes `""` (especially if they contain spaces, commas, or special characters).

The connector provides logging with:
- Timestamps
- Log levels (INFO, WARNING, ERROR)
- Detailed operation status
- Message payload never displayed in logs

Example log output:
```
2026-08-07 11:04:12,298 | INFO | Starting Kafka Gen check
2026-08-07 11:04:12,298 | INFO | Servers           : localhost:9092
2026-08-07 11:04:12,298 | INFO | Topic             : demo-topic
2026-08-07 11:04:12,298 | INFO | Group ID          : vtom-gen
2026-08-07 11:04:12,298 | INFO | Expected text     : VTOM_ID
2026-08-07 11:04:12,310 | INFO | Successfully connected to Kafka
2026-08-07 11:04:15,509 | INFO | Assigned partitions: demo-topic:0
2026-08-07 11:04:15,849 | INFO | Matching message found
2026-08-07 11:04:15,849 | INFO | Result: OK (code 0)
```

# License
This project is licensed under Apache 2.0. See the [LICENSE](LICENSE) file for more details.

# Code of Conduct
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.1%20adopted-ff69b4.svg)](code-of-conduct.md)
Absyss SAS has adopted the [Contributor Covenant](CODE_OF_CONDUCT.md) as a Code of Conduct and expects project participants to adhere to it as well. Please read the [complete document](CODE_OF_CONDUCT.md) to understand which actions will or will not be tolerated.
