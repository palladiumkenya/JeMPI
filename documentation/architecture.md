---
description: How it works
---

# Architecture

The JeMPI Client Registry is a system that incorporates a microservice architecture, each microservice has a specific task such as data cleaning, data storing, etc. These various services communicate through a Kafka message bus, meaning that every service is storing and retrieving data from a specific Kafka topic.

**Below the synchronous and asynchronous flow diagram.**

#### [Asynchronous flow](https://drive.google.com/file/d/1rcbF3UJ5Lh-4bjXl8GVpJVnYxA1diRjl/view?usp=sharing) <a href="#_2v012h2bohjt" id="_2v012h2bohjt"></a>

![](.gitbook/assets/0)

## JeMPI_AsyncReceiver <a href="#_6om7ih1t1k41" id="_6om7ih1t1k41"></a>

**Description:** A microservice that sends the content of an uploaded csv file to the JeMPI_ETL service. the JeMPI_AsyncReciever service produces kafka messages where each message has a row from the CSV file uploaded. it will then be saved under a kafka topic.

The base version of JeMPI supports only 10 columns in the following order **\[for the current version]**:

**String** uid,\
**SourceId** sourceId,\
**String** auxId,\
**String** givenName,\
**String** familyName,\
**String** gender,\
**String** dob,\
**String** city,\
**String** phoneNumber,\
**String** nationalId

**Input**

1. A CSV file located in the JeMPI_AsyncReciever associated volume, under \*/app/csv\_ directory (this can be done through HTTP request).\
   Example of input file:

```
ID,Given_Name,Family_Name,Gender_at_Birth,Date_of_Birth,City,Phone_Number,National_ID,Dummy1,Dummy2,Dummy3
rec-00000000-aaa-0,Endalekachew,Onyango,male,20171114,Nairobi,091-749-4674,198804042874913,19940613,19781023,19660406
rec-00000001-aaa-0,Fikadu,Mwendwa,male,19840626,Nairobi,022-460-8846,199403050409528,20190317,19400321,20190104
rec-00000002-bbb-0,Biniyam,Maalim,male,20191022,Nairobi,098-119-7244,200006231841948,,20190302,
```

**Output**

The service will save the data from the CSV file, one line at a time.\
Kafka topic: _TOPIC_INTERACTION_ASYNC_ETL="JeMPI-async-etl"_

## JeMPI_ETL <a href="#_r783bgaxx08b" id="_r783bgaxx08b"></a>

**Description:** A microservice that pocesses the input coming from the JeMPI_AsyncReceiver. The JeMPI_ETL service will perform some data trasformation e.g. lower case the values for name of the patient or unformat the date for the date of birth. The resulting data will be sent as JSON (JSON Streaming) to the JeMPI_Controller service.

**Input:**

Data coming from the the JeMPI_AsyncReciever service.\
Kafka topic: \_TOPIC_PATIENT_ASYNC_PREPROCESSOR="JeMPI-async-etl"\*

**Output:**

Data transformed into JSON that will be sent to the JeMPI_Controller. It will be stored in the Kafka topic: \_TOPIC_PATIENT_CONTROLLER="JeMPI-patient-controller"\*

Example or a Kafka message coming from the patient controller topic:

<figure><img src=".gitbook/assets/3" alt=""><figcaption></figcaption></figure>

```json
{
  "contentType": "BATCH_INTERACTION",
  "tag": "csv/import-1050836091564327170.csv",
  "stan": "2023/09/06 08:29:13:0000008",
  "interaction": {
    "sourceId": { "facility": "FA4", "patient": "197910145001067" },
    "uniqueInteractionData": {
      "auxDateCreated": "2023-09-06T08:29:13.426518561",
      "auxId": "rec-0000000002--5",
      "auxClinicalData": "RANDOM DATA(975)"
    },
    "demographicData": {
      "givenName": "esther",
      "familyName": "zulu",
      "gender": "female",
      "dob": "19791014",
      "city": "mufulira",
      "phoneNumber": "0157172342",
      "nationalId": "197910145001067"
    }
  }
}
```

## JeMPI_Controller <a href="#_lpn0tn79g4ka" id="_lpn0tn79g4ka"></a>

**Description:** The JeMPI_Controller service has multiple tasks:

- Send the data coming from the JeMPI_ETL to both the JeMPI_Linker and the JeMPI_EM services. The data will be stored in their respective Kafka topics accessed (consumed) by those service.
- Control and manage the optimization of the M & U value computing by activating or stopping the linkage process of the JeMPI_Linker service. The new values of M & U will be brought from JeMPI_EM service to then be provided to the JeMPI_Linker service.

**Input:**
Data coming from the JeMPI_ETL service
Kafka topic: \_TOPIC_PATIENT_CONTROLLER="JeMPI-patient-controller"\*.

Values of the M & U computed in the JeMPI_EM service
Kafka topic: \_TOPIC_MU_CONTROLLER="JeMPI-mu-controller"\*

**Output:**
Send the data to the JeMPI_EM

- Kafka topic: _TOPIC_PATIENT_EM="JeMPI-patient-em"_
  Send the data to the JeMPI_Linker
- Kafka topic: _TOPIC_PATIENT_LINKER="JeMPI-patient-linker"_

MU process: Kafka topic: _TOPIC_MU_LINKER="JeMPI-mu-linker"_

![](.gitbook/assets/4) ![](.gitbook/assets/5)

## JeMPI_EM <a href="#_7tf3t1atn1ab" id="_7tf3t1atn1ab"></a>

**Description:** A microservice that will create an object containing m\&u of a patient against patient records that go into the EM algorithm (quality (m) and the uniqueness (u) per field). This object is used in the linker for matching patients. It uses a machine learning called Estimation maximisation (EM) algorithm to optimize that value, it is launched after receiving a number of records specified in the configuration.

**Input:** Kafka topic: _TOPIC_PATIENT_LINKER="JeMPI-patient-linker"_

**Output:** Kafka topic: _TOPIC_MU_CONTROLLER="JeMPI-mu-controller"_

## JeMPI_Linker <a href="#_111ah0ssrp64" id="_111ah0ssrp64"></a>

**Description:** A microservice that will interact with Dgraph database to do the matching of the patients. The Linker uses thresholds to drive the linking and notifications for review processes. These thresholds are the following:

- **A single match or no match threshold :** the encounter will automatically be linked to the highest golden record candidate above the threshold. If no candidate has a score above the threshold, a new golden record is created. This is typically used for fully autonomous linking.
- **Window around the match/no match threshold :** if the highest score generated for the candidates falls within this window, a notification is sent for Admin to review the encounter.
- **Margin threshold :** if another candidate falls within a margin from the highest score and this highest score is above the match/no match threshold, a notification for review is sent for the Admin to review the linked encounter.

**Input:**
Kafka topic: _TOPIC_PATIENT_EM="JeMPI-patient-em"_\
Kafka topic: _TOPIC_MU_LINKER="JeMPI-mu-linker"_

**Output:**

- Interact with the Dgraph database using GraphQL queries/mutations, save the patients and the links.
- Send response of either the link info or the list of candidates to the Controller
- Save response to Kafka topic: _TOPIC_notifications=”JeMPI_notifications”_

## JeMPI_Dgraph <a href="#_kb1wgk9uafqz" id="_kb1wgk9uafqz"></a>

**Description:** The Dgraph database used for JeMPI to store the patient records. it is a graph database.

Component linked:

- **Dgraph Ratel:** A tool for data visualization and cluster management. Ratel can be used with Dgraph to manage cluster settings, run DQL queries and mutations and see results of the mentioned operations.
- **Dgraph Alpha:** Expose and host endpoints of the indexes.
- **Dgraph Zero:** it is like a zookeeper in Kafka, it will control the instances of Alpha by assigning them to a group, and re-balances the data between them.

## JeMPI_Kafka <a href="#_lhpqpufx5pyy" id="_lhpqpufx5pyy"></a>

**Description:** Kafka the message queue bus, it contains all the topics used previously in the other components.

## JeMPI_API <a href="#_ioszcxv7tpj" id="_ioszcxv7tpj"></a>

**Description:** The JeMPI_API service contains the endpoints needed to interact with JeMPI. aside from acting as an access point to the JeMPI system, this service

It will do the following actions:

- Read data from the Kafka topic _TOPIC_notifications=”JeMPI_notifications”_
- Save data related to the administration in PostgeSQL DB
- Get the data from PostgreSQL when the JeMPI Web requests data.
