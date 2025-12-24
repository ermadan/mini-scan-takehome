# Mini-Scan

## Problem 

- Horizonatally-scalable application that saves messages from Pub/Sub into sqlite database. 
- at-least-once semantics
- handles out-of-order messages 

## Solution

1. Received messages are accumulated and written/processed in a batches.
2. Failed to be inserted message 
3. Out-of-order messages are ingnored using sqlite sql ON CONFlICT clause 

## Running

`docker compose up`
Scan processor service configuration is in `docker-compose.yml`, configurable values:
- BATCH_SIZE, int
- TEST_ORDER, string, if set it will create entry with the TEST_ORDER value as ip and `math.MaxUint32` as timestamp, and substitute the . It will also print top 20 entries from the table ordered by timestamp desc


## Task Definition

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

Your job is to build the data processing side. It should:

1. Pull scan results from the subscription `scan-sub`.
2. Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> **_NOTE_**
> The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
>
> ```javascript
> {
>   // ...
>   "data_version": 1,
>   "data": {
>     "response_bytes_utf8": "aGVsbG8gd29ybGQ="
>   }
> }
>
> {
>   // ...
>   "data_version": 2,
>   "data": {
>     "response_str": "hello world"
>   }
> }
> ```

Your processing application should be able to be scaled horizontally, but this isn't something you need to actually do. The processing application should use `at-least-once` semantics where ever applicable.

You may write this in any languages you choose, but Go would be preferred.

You may use any data store of your choosing, with `sqlite` being one example. Like our own code, we expect the code structure to make it easy to switch data stores.

Please note that Google Pub/Sub is best effort ordering and we want to keep the latest scan. While the example scanner does not publish scans at a rate where this would be an issue, we expect the application to be able to handle extreme out of orderness. Consider what would happen if the application received a scan that is 24 hours old.

cmd/scanner/main.go should not be modified

---

Please upload the code to a publicly accessible GitHub, GitLab or other public code repository account. This README file should be updated, briefly documenting your solution. Like our own code, we expect testing instructions: whether it’s an automated test framework, or simple manual steps.

To help set expectations, we believe you should aim to take no more than 4 hours on this task.

We understand that you have other responsibilities, so if you think you’ll need more than 5 business days, just let us know when you expect to send a reply.

Please don’t hesitate to ask any follow-up questions for clarification.
