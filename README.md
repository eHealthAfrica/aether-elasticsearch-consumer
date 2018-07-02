# Aether Connect SDK Example
Aether Output Connectors are a powerful way of feeding foreign systems with data from an Aether instance. Part of our approach with Aether is to provide simple access to powerful functionality without being overly opinionated. With the Aether Connect SDK, we provide a low level Python API to Kafka Topics running on the instance. On top of the base functionality you would expect from a Kafka Consumer, we provide a number of additional features.

  - Fast deserialization of messages with Spavro
  - Value based filtering of whole messages.
  - Exclusion of fields from a read message based on data classification

With these SDK examples, we aim to provide best practices for structuring a Output Connect container, along with tests and everything you need to deploy.
