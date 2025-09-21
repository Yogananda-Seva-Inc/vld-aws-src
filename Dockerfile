FROM public.ecr.aws/lambda/python:3.13
#FROM public.ecr.aws/amazonlinux/amazonlinux:latest

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY user_functions.py ${LAMBDA_TASK_ROOT}
COPY utility_functions.py ${LAMBDA_TASK_ROOT}
COPY zoom_download.py ${LAMBDA_TASK_ROOT}
COPY s3_upload.py ${LAMBDA_TASK_ROOT}
COPY s3_upload_fact_conn.py ${LAMBDA_TASK_ROOT}
COPY s3_upload_fact_uuid.py ${LAMBDA_TASK_ROOT}
COPY s3_upload_staging_fact_conn.py ${LAMBDA_TASK_ROOT}
COPY s3_upload_staging_fact_uuid.py ${LAMBDA_TASK_ROOT}
COPY config.json ${LAMBDA_TASK_ROOT}
COPY fact_uuid_master.csv ${LAMBDA_TASK_ROOT}
COPY run_all.py ${LAMBDA_TASK_ROOT}

#ENTRYPOINT [ "/bin/bash" ]
ENTRYPOINT [ "python3", "run_all.py", "run" ]

# # Update installed packages and install Apache
# RUN yum update -y && \
#  yum install -y httpd

# # Write hello world message
# RUN echo 'Hello World!' > /var/www/html/index.html

# # Configure Apache
# RUN echo 'mkdir -p /var/run/httpd' >> /root/run_apache.sh && \
#  echo 'mkdir -p /var/lock/httpd' >> /root/run_apache.sh && \
#  echo '/usr/sbin/httpd -D FOREGROUND' >> /root/run_apache.sh && \
#  chmod 755 /root/run_apache.sh

# EXPOSE 80

# CMD /root/run_apache.sh
