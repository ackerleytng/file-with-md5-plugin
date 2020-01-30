from airflow.hooks.base_hook import BaseHook


class FileWithMd5Hook(BaseHook):
    """
    Allows for interaction with a file and an md5sum file on airflow's filesystem

    Connection should have a file_glob and md5_glob specified under extra

    file_glob should match only _one_ file
    md5_glob is optional and defaults to f"{file_glob}.md5sums"

    The md5sums file is the format output by

    md5sum ${file} > ${file}.md5sums

    Example:
    Conn Id: sample_file_with_md5
    Conn Type: File (path)
    Host, Schema, Login, Password, Port: empty
    Extra: {"file_glob": "/data/csv/*sales.csv", "md5_glob": "/data/md5sums/*sales.csv.md5sums"}
    """
    def __init__(self, conn_id="file_with_md5_default"):
        conn = self.get_connection(conn_id)
        self.file_glob = conn.extra_dejson.get("file_glob")
        self.md5_glob = conn.extra_dejson.get("md5_glob", f"{self.file_glob}.md5sums")

    def get_file_glob(self):
        return self.file_glob

    def get_md5_glob(self):
        return self.md5_glob
