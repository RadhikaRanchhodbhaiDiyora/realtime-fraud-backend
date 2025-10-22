## Real-Time Fraud Detection Backend

**Architecture**: Kafka, Zookeeper, Redis, Producer, Processor

**Setup/Running Instructions**:
1. Download the dataset from here: https://www.kaggle.com/datasets/kartik2112/fraud-detection?select=fraudTrain.csv
2. Download and open the **realtime-fraud-backend** folder in vs code.
3. create a new folder named **data** inside the folder and place the downloaded dataset fraudTrain.csv inside it.
4. Install docker desktop and start it.
5. Open a new terminal in vs code and then run the below command:

	docker-compose up --build

**Requirements**: Docker Desktop, VS Code.

**Note**: In phase 2 the complete pipeline checked by limiting the data rows to first 50 rows (check producer.py, line 39).
The process will continue until the Data Producer (producer service) runs out of data to send from the fraudTrain.csv file.
The dataset consists of approx. 1.3 million rows.
