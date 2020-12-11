FROM continuumio/miniconda3 as python_env
WORKDIR /
COPY environment.yml .
RUN conda env create -f environment.yml

# FROM continuumio/miniconda3
FROM python_env
WORKDIR /code

ENV CONDA_ENV myenv
ENTRYPOINT [ "/code/entry.sh" ]

CMD python -u web/dashboard.py