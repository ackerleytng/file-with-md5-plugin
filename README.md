# file_with_md5_plugin

A plugin for airflow with a sensor for the existence of a file based on a glob and then checks its md5sum.

Useful when transferring large files into a watched directory, to prevent airflow from operating on the file when the large file has not finished transferring.

# Quickstart

Deploy the the plugin, then deploy the example dag

```
cp examples/example_dag.py $AIRFLOW_HOME/dags
```

Add the `sales_csv` connection:

```
airflow connections --add --conn_id sales_csv --conn_type fs --conn_extra '{"file_glob": "/tmp/input/*sales.csv", "md5_glob": "/tmp/input/*sales.csv.md5sums"}'
```

Create the input files

```
mkdir /tmp/input
echo 'apple,1' > /tmp/input/2020-01-01-sales.csv
md5sum /tmp/input/2020-01-01-sales.csv > /tmp/input/2020-01-01-sales.csv.md5sums
```

The dag should run successfully, and you should see something like this among your logs

```
$ cat logs/file_with_md5_example/do_something_with_file/2020-01-30T01\:53\:49.143837+00\:00/1.log
< ... elided ... >
[2020-01-30 01:54:23,186] {{logging_mixin.py:112}} INFO - /tmp/input/2020-01-01-sales.csv
[2020-01-30 01:54:23,186] {{logging_mixin.py:112}} INFO - apple,1
[2020-01-30 01:54:23,186] {{logging_mixin.py:112}} INFO -
[2020-01-30 01:54:23,186] {{logging_mixin.py:112}} INFO - /tmp/input/2020-01-01-sales.csv.md5sums
[2020-01-30 01:54:23,186] {{logging_mixin.py:112}} INFO - 97fa459850a07d34f629ee741d9030d4  2020-01-01-sales.csv
[2020-01-30 01:54:23,186] {{logging_mixin.py:112}} INFO -
< ... elided ... >
$
```

# Deploying

```
cp -r file_with_md5_plugin $AIRFLOW_HOME/plugins
```

# Testing

From the root directory of this project, run

```
python -m pytest -vvsx
```
