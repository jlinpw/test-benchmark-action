FROM python
RUN pip install requests
COPY . /app
COPY resource.json /app/resource.json 
WORKDIR /app
RUN ["wget", "https://raw.githubusercontent.com/parallelworks/pw-cluster-automation/master/client.py", "-O", "/app/client.py"]
ENTRYPOINT [ "python", "-u", "/app/run_workflow.py" ]