### Contents
A pipeline that fetches OpenWeather API data and writes them to a GCS bucket and afterwards to an instance of Google BigQuery database.
### Running
Execute the following code by creating a Google Cloud account and creating a project. <br>
The user needs to create instances of a:
<li>bucket in Cloud Storage</li>
<li>composer environment in Composer</li>
<li>table in Big Query</li>

Then he must update accordingly the variables representing those names in ``cloud.py`` file. <br>
Then the file must be uploaded under the ``dags`` folder, so Cloud Composer recognizes it as a DAG instance.
To run composer click on the ``Environment configuration`` tab and there you'll find the URL that will redirect you to
an airflow-like UI.
<br><br>
Now you may find your dag under the id ``cloud``.
