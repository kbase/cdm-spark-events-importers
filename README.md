# CDM Spark events importers

**This is currently a prototype**

Importer code to run based on an event in the
[CDM Spark Event Processor (CSEP)](https://github.com/kbase/cdm-spark-events).

Currently the CSEP responds to jobs completing in the
[CDM Task Service (CTS)](https://github.com/kbase/cdm-task-service)

## Adding an importer

### Working example

There is a working example of a CheckM2 importer
[YAML specification file](./cdmeventimporters/checkm2.yaml),
[import code](./cdmeventimporters/checkm2.py), and [tests](./test/checkm2_test.py) in this repo.
As of this writing they are the first attempt at an importer and will likely improve over time,
so it is recommended to check back every few hours.

### Instructions

To add an importer two files must be added under the `cdmeventimporters` directory:

* A python module file that performs the import of the CTS output into Spark Deltatables.
    * The module must contain a `run_import` top level method that takes three
      parameters:
        * A function, that when called, returns a SparkSession correctly configured
          with S3 Deltatable information and credentials.
            * The function takes one keyword only argument: `executor_cores`, which
              is an integer that specifies the number of CPU cores for each Spark
              executor. The default is one, which in many cases is likely enough
              since most import work should be IO heavy.
        * Information about the job - see the data structure documentation below.
        * The metadata, if any, from the YAML file (see below).
* A YAML file that specifies when the importer should run (see below).

#### Job information data structure

The data structure provided in the 2nd argument to `run_import` is a dictionary:

```
{
    "id": <the CTS job ID>,
    "outputs": [
        {
            "file": <the S3 file path for the CTS output file starting with the bucket>,
            "crc64nvme: <the base64 encoded crc64nvme file checksum>,
        },
        ...
    ],
    "image": <the image name of CTS Docker image that was run as part of the job>,
    "image_digest": <the digest of the image>,
    "input_file_count": <the number of input files for the job>,
    "output_file_count": <the number of output files for the job>,
    "completion_time": <the time the job completed as an ISO8601 string>,
}
```

For a simple importer probably only the `'id` and `outputs` fields are necessary. The job ID
should be included in the deltatable for data lineage tracking purposes.

#### YAML file structure

The major purpose of the YAML file is to map CTS image names to importer code so that the event
processor knows what importer code to run for a given job. The structure looks like this:

```
# The name of the importer. Informational purposes only.
name: <an arbitrary name for the importer>
# The location of the importer module containing the `run_import` method in python
# module format, e.g. "cdmeventimporters.my_module"
py_module: <the importer module>
# The image associated with the importer. This importer will run when a CTS job completes
# with this image. Example: ghcr.io/kbasetest/cdm_checkm2
image:  <CTS Docker image name>
# Metadata for the importer. These are currently arbitrary key value pairs, and
# are provided to the importer in the 3rd parameter to run_import.
importer_meta:
    <key1>: <value1>,
    ...
    <keyN>: <valueN>,
```

Note that multiple YAML files cannot reference the same Docker image.

### Implementation notes

* In almost all cases, the importers should be reading CSV / TSV files or their equivalents
  and writing them into Deltatables and not much else. Any heavy computational lifting should
  be done in the CTS job. The importers should use `pyspark`, `delta-spark` and not much
  else for processing.
* Do not assume that the event will only occur once. In the case of upstream failures, an
  event may be provided to the importer more than once - the importer should take this into
  account and prevent adding duplicate data to the database. the CheckM2 example code shows
  how to handle this.
* The same importer is called for all versions of a Docker image. If data needs to be processed
  differently for different versions of the image, it is the importer's responsibility to do so.
  The image digest is provided for this reason. The image tag is deliberately not provided
  as it is mutable.

## Development

### Adding code

* In this alpha / prototype stage, we will be PRing (do not push directly) to `main`. In the
  future, once we want to deploy beyond CI, we will add a `develop` branch.
* The PR creator merges the PR and deletes branches (after builds / tests / linters complete).

### Code requirements for prototype code

* Any code committed must at least have a test file that imports it and runs a noop test so that
  the code is shown with no coverage in the coverage statistics. This will make it clear what
  code needs tests when we move beyond the prototype stage.
* Each module should have its own test file. Eventually these will be expanded into unit tests
* Any code committed must have regular code and user documentation so that future devs
  converting the code to production can understand it.
* Release notes are not strictly necessary while deploying to CI, but a concrete version (e.g.
  no `-dev*` or `-prototype*` suffix) will be required outside of that environment. On a case by
  case basis, add release notes and bump the prototype version (e.g. 0.1.0-prototype3 ->
  0.1.0-prototype4) for changes that should be documented.

### Running tests

```
docker compose up -d --build
docker compose exec test-container pytest test
docker compose down  # optionally, leave up if you want to test again
```

### Exit from prototype status

* Coverage badge in Readme
* Run through all code, refactor to production quality
* Add tests where missing (which is a lot) and inspect current tests for completeness and quality
  * E.g. don't assume existing tests are any good
