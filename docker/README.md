# Docker for dbt
This docker file is suitable for building dbt Docker images locally or using with CI/CD to automate populating a container registry.


## Building an image:
This Dockerfile can create images for the following target: `dbt-snowflake`

In order to build a new image, run the following docker command.
```
docker build --tag <your_image_name> --target dbt-snowflake <path/to/dockerfile>
```
---
> **Note:**  Docker must be configured to use [BuildKit](https://docs.docker.com/develop/develop-images/build_enhancements/) in order for images to build properly!

---

By default the images will be populated with the most recent release of `dbt-snowflake`.  If you need to use a different version you can specify it by git ref using the `--build-arg` flag:
```
docker build --tag <your_image_name> \
  --target dbt-snowflake \
  --build-arg dbt_snowflake_ref=<git_ref> \
  <path/to/dockerfile>
```

### Examples:
To build an image named "my-dbt" that supports Snowflake using the latest releases:
```
cd dbt-core/docker
docker build --tag my-dbt --target dbt-snowflake .
```

To build an image named "my-other-dbt" that supports Snowflake using the adapter version 1.0.0b1:
```
cd dbt-core/docker
docker build \
  --tag my-other-dbt \
  --target dbt-snowflake \
  --build-arg dbt_snowflake_ref=dbt-snowflake@v1.0.0b1 \
 .
```

## Special cases
There are a few special cases worth noting:

* If you need to build against another architecture (linux/arm64 in this example) you can override the `build_for` build arg:
```
docker build --tag my_dbt \
  --target dbt-snowflake \
  --build-arg build_for=linux/arm64 \
  <path/to/dockerfile>
```

Supported architectures can be found in the python docker [dockerhub page](https://hub.docker.com/_/python).

## Running an image in a container:
The `ENTRYPOINT` for this Dockerfile is the command `dbt` so you can bind-mount your project to `/usr/app` and use dbt as normal:
```
docker run \
  --network=host \
  --mount type=bind,source=path/to/project,target=/usr/app \
  --mount type=bind,source=path/to/profiles.yml,target=/root/.dbt/profiles.yml \
  my-dbt \
  ls
```
---
**Notes:**
* Bind-mount sources _must_ be an absolute path
* You may need to make adjustments to the docker networking setting depending on the specifics of your data warehouse/database host.

---
