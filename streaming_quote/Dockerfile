FROM mcr.microsoft.com/azure-cli:latest

# Set the working directory inside the container
WORKDIR /src

COPY . .

# Install necessary packages using apk
RUN apk update && \
    apk add --no-cache nano \
                       busybox \
                       python3 \
                       curl \
                       bash \
                       build-base


#Install Poetry 
RUN pip install poetry

#Adjust PATH to include Poetry's bin directory
ENV PATH="${PATH}:/root/.poetry/bin"

#Copy poetry configuration files 
COPY pyproject.toml poetry.lock* ./

#Intsall project dependencies using Poetry
RUN poetry install 

COPY setup_crontab.sh /src/setup_crontab.sh 

RUN chmod +x /src/setup_crontab.sh

RUN /bin/bash -c "/src/setup_crontab.sh"

#Set the default command to run when a container starts 
CMD ["crond", "-f", "-d", "8"]
