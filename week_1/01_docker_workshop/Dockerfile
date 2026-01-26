# this is a base image, it contains an OS and python installed
FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/ 



# install dependencies
# RUN pip install pandas pyarrow

# set the working directory inside the container, if the folder does not exist, it will be created
WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"
COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

# this means copy the file from the host to the container
COPY pipeline/ingest_data.py . 

# this means when the container starts, run this command
ENTRYPOINT ["python", "ingest_data.py"]