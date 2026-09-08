# Connecteur Kafka Gen
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](../../LICENSE.md)&nbsp;
[![fr](https://img.shields.io/badge/lang-en-red.svg)](README.md)

La Ressource Générique effectue un contrôle si le message attendu est déjà présent sur un topic Kafka et retourne immediatement un succès/echec.

# Disclaimer
Aucun support ni garanties ne seront fournis par Absyss SAS pour ce projet et fichiers associes. L'utilisation est a vos propres risques.

Absyss SAS ne peut etre tenu responsable des dommages causes par l'utilisation d'un des fichiers mis a disposition dans ce depot Github.

Il est possible de faire appel a des jours de consulting pour l'implementation.

# Prerequis

  * Visual TOM 7.3.2 ou plus
  * Python 3.x ou plus
  * Acces a un cluster / topic Kafka
  * Installer les packages python requis avec pip: `pip install -r requirements.txt`

# Consignes
  * Windows :
    * Placer le fichier `kafkaGen.py` dans le repertoire `%ABM_BIN%`
    * Declarer la queue / application custom sur l'agent (voir guide administrateur)

  * Linux/Unix :
    * Placer le fichier `kafkaGen.py` dans le repertoire `$TOM_ADMIN`
    * Declarer la queue / application custom sur l'agent (voir guide administrateur)

  * Créer dans Visual TOM une "Ressource Générique" avec la définition suivante ou importer le fichier KafkaGen.xml:
   ```
   vtimport -x -f KafkaGen.xml
   ```
   ![Custom application screenshot](screenshots/kafkaGen.png?raw=true)

Le script retourne des codes de sortie specifiques :
- `0` : Message correspondant trouve
- `1` : Aucun message correspondant / erreur

Description des parametres:
  * `Param 1` BOOTSTRAP_SERVERS : serveurs Kafka (requis), ex: `localhost:9092`
  * `Param 2` TOPIC : nom du topic (requis)
  * `Param 3` GROUP_ID : identifiant du consumer group (requis, cree automatiquement)
  * `Param 4` EXPECTED_TEXT : chaine attendue dans le message (requis)
  * `Param 5` COMMIT_MODE : mode de commit optionnel (defaut `0`)
    * `0` : pas de commit
    * `1` : commit du lot lu
    * `2` : commit jusqu'au premier message trouve
  * `Param 6` EXPECTED_IDS : IDs optionnels attendus, separes par `,`
  * `Param 7` SECURITY_PROTOCOL : optionnel (`PLAINTEXT` / `SASL_SSL` / `SSL`, defaut `PLAINTEXT`)
  * `Param 8` SASL_MECHANISM : mecanisme SASL optionnel (ex: `PLAIN`)
  * `Param 9` USERNAME : utilisateur SASL optionnel
  * `Param 10` PASSWORD : mot de passe SASL optionnel
  * `Param 11` SSL_CAFILE : chemin optionnel du certificat CA

Protegez les parametres avec des guillemets `""` (surtout s'ils contiennent des espaces, des virgules ou des caracteres speciaux).

Le connecteur fournit une journalisation avec :
- Horodatage
- Niveaux de log (INFO, WARNING, ERROR)
- Statut detaille des operations
- Contenu des messages jamais affiché dans les logs

Exemple de sortie de log :
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

# Licence
Ce projet est sous licence Apache 2.0. Voir le fichier [LICENCE](LICENSE) pour plus de details.

# Code de conduite
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.1%20adopted-ff69b4.svg)](code-of-conduct.md)
Absyss SAS a adopte le [Contributor Covenant](CODE_OF_CONDUCT.md) en tant que Code de Conduite et s'attend a ce que les participants au projet y adherents egalement. Merci de lire le [document complet](CODE_OF_CONDUCT.md) pour comprendre les actions qui seront ou ne seront pas tolerees.
