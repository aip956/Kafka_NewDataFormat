To run:
cd into Kafka_Consumer_Producer_NewDataFormat
start Docker

In terminal:
Start/build the container:
docker-compose up -d
Start the app:
uvicorn main:app --reload
(you might need to save a file to get it to reload)
open another terminal
run the sim file: python3 sim_Kafka.py events_data1.txt

Access FastAPI Swagger:
http://127.0.0.1:8000/docs
http://localhost:8000/docs

#### Post a message:
topic: feeling_ill
message: Guest has stomach ache after eating 5 pieces of cake
- Should return code 200
- Alternatively, can start the servers (docker-compose up -d) and the app (uvicorn main:app --reload)
-- Then run data script to auto-send data:

**python3 sim_Kafka.py events_data1.txt**
sim_Kafka will run the data file
events_data.txt is the data file


Get the consumed messages:
Get, execute
{
  "messages": [
    {
      "topic": "my_topic",
      "message": "Hello Kafka!"
    }
  ]
}

Stop process in foreground:
Control-C 
Gracefully shut down:
docker-compose stop
Remove containers:
docker-compose down

git checkout main
git pull
git checkout -b anthea/subject

Make a branch
Commit
