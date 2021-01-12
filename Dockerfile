FROM python

WORKDIR /usr/src/app/docker

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./script_download_dados.py" ]

