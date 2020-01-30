import logging

from file_with_md5_plugin.sensors.file_with_md5_sensor import FileWithMd5Sensor


def test__get_file_from_glob(tmpdir):
    csv = tmpdir.join("2020-01-01-sales.csv")
    csv.write("apple,1")

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    g = f"{tmpdir.realpath()}/*sales.csv"
    f = s._get_file_from_glob(g)

    assert csv.realpath() == f


def test__get_file_from_glob_no_files(tmpdir):
    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    g = f"{tmpdir.realpath()}/*sales.csv"
    f = s._get_file_from_glob(g)

    assert f is None


def test__get_file_from_glob_more_than_1_file(tmpdir):
    tmpdir.join("2020-01-01-sales.csv").ensure()
    tmpdir.join("2020-02-01-sales.csv").ensure()

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    g = f"{tmpdir.realpath()}/*sales.csv"
    f = s._get_file_from_glob(g)

    assert f is None


def test__get_file_from_glob_dir_doesnt_exist():
    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    g = f"/some/path/that/doesnt/exist/*.sql"
    f = s._get_file_from_glob(g)

    assert f is None


def test__md5_file(tmpdir):
    csv = tmpdir.join("2020-01-01-sales.csv")
    csv.write("apple,1")

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    assert s._md5_file(csv.realpath()) == "e81370ac6af0feaccdb3a9e70165da90"


def test__read_md5file(tmpdir):
    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "286755fad04869ca523320acce0dc6a4  2020-01-01-sales.csv\n"
        "30c6677b833454ad2df762d3c98d2409  2020-02-01-sales.csv\n"
    )

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    md5 = s._read_md5file(hash_file.realpath(), "2020-01-01-sales.csv")

    assert md5 == "286755fad04869ca523320acce0dc6a4"


def test__read_md5file_file_does_not_exist(tmpdir):
    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    md5 = s._read_md5file("/path/does/not/exist", "2020-01-01-sales.csv")

    assert md5 is None


def test__read_md5file_file_malformed(tmpdir):
    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "not_md5_hash_01  2020-01-01-sales.csv\n"
        "not_md5_hash_02  2020-02-01-sales.csv\n"
    )

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    md5 = s._read_md5file(hash_file.realpath(), "2020-01-01-sales.csv")

    assert md5 is None


def test__read_md5file_md5_missing(tmpdir):
    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "286755fad04869ca523320acce0dc6a4  2020-01-01-sales.csv\n"
        "30c6677b833454ad2df762d3c98d2409  2020-02-01-sales.csv\n"
    )

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    md5 = s._read_md5file(hash_file.realpath(), "some-other-file.csv")

    assert md5 is None


def test__poke(tmpdir):
    csv_file = tmpdir.join("2020-01-01-sales.csv")
    csv_file.write("password\n")

    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "286755fad04869ca523320acce0dc6a4  2020-01-01-sales.csv\n"
        "30c6677b833454ad2df762d3c98d2409  2020-02-01-sales.csv\n"
    )

    csv_glob = f"{tmpdir.realpath()}/*sales.csv"
    hash_glob = f"{tmpdir.realpath()}/*sales.csv.md5sums"

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")
    expected = (
        f"{tmpdir.realpath()}/2020-01-01-sales.csv",
        f"{tmpdir.realpath()}/2020-01-01-sales.csv.md5sums"
    )

    assert s._poke(csv_glob, hash_glob) == expected


def test__poke_no_match(tmpdir):
    csv_file = tmpdir.join("2020-01-01-sales.csv")
    csv_file.write("no match")

    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "286755fad04869ca523320acce0dc6a4  2020-01-01-sales.csv\n"
        "30c6677b833454ad2df762d3c98d2409  2020-02-01-sales.csv\n"
    )

    csv_glob = f"{tmpdir.realpath()}/*sales.csv"
    hash_glob = f"{tmpdir.realpath()}/*sales.csv.md5sums"

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    assert s._poke(csv_glob, hash_glob) is None


def test__poke_file_missing(tmpdir):
    csv_file = tmpdir.join("2020-01-01-sales.csv")
    csv_file.write("no match")

    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "286755fad04869ca523320acce0dc6a4  2020-01-01-sales.csv\n"
        "30c6677b833454ad2df762d3c98d2409  2020-02-01-sales.csv\n"
    )

    csv_glob = f"{tmpdir.realpath()}/*sales-missing.csv"
    hash_glob = f"{tmpdir.realpath()}/*sales.csv.md5sums"

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    assert s._poke(csv_glob, hash_glob) is None


def test__poke_hash_file_missing(tmpdir):
    csv_file = tmpdir.join("2020-01-01-sales.csv")
    csv_file.write("no match")

    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "286755fad04869ca523320acce0dc6a4  2020-01-01-sales.csv\n"
        "30c6677b833454ad2df762d3c98d2409  2020-02-01-sales.csv\n"
    )

    csv_glob = f"{tmpdir.realpath()}/*sales.csv"
    hash_glob = f"{tmpdir.realpath()}/*sales-missing.csv.md5sums"

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    assert s._poke(csv_glob, hash_glob) is None


def test__poke_md5_missing(tmpdir):
    csv_file = tmpdir.join("2020-01-01-sales.csv")
    csv_file.write("no match")

    hash_file = tmpdir.join("2020-01-01-sales.csv.md5sums")
    hash_file.write(
        "286755fad04869ca523320acce0dc6a4  2020-03-01-sales.csv\n"
        "30c6677b833454ad2df762d3c98d2409  2020-02-01-sales.csv\n"
    )

    csv_glob = f"{tmpdir.realpath()}/*sales.csv"
    hash_glob = f"{tmpdir.realpath()}/*sales.csv.md5sums"

    s = FileWithMd5Sensor(task_id="placeholder_task_id", conn_id="placeholder_conn_id")

    assert s._poke(csv_glob, hash_glob) is None
