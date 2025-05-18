ARG version=dev

FROM python:3.12-alpine

ENV APP_USER=www-data

RUN apk add python3-dev libffi-dev build-base linux-headers pcre-dev

# Install uWSGI
RUN pip install uwsgi

# Overwrite the uWSGI config
COPY docker/flask/uwsgi.ini /etc/uwsgi/

COPY . /src

RUN pip install --requirement /src/requirements.txt

RUN chmod 755 /src/docker/flask/start_flask.sh

WORKDIR /src

RUN adduser -S $APP_USER -G $APP_USER

RUN chown -R $APP_USER:$APP_USER /src

USER $APP_USER

CMD ["sh", "/src/docker/flask/start_flask.sh"]
