# # Use an image with curl installed to download the file
# FROM debian:bullseye-slim AS downloader
#
# # Install curl
# RUN apt-get update && apt-get install -y curl && apt-get clean
#
# # Download the file from the URL
# RUN curl -o /tmp/global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

FROM public.ecr.aws/lambda/java:11

# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
# ENV PATH=$JAVA_HOME/bin:$PATH

#copy java default cacerts
COPY src/main/resources/cacerts /usr/lib/jvm/java-11-openjdk/lib/security/cacerts
RUN chmod u+w /usr/lib/jvm/java-11-openjdk/lib/security/cacerts
#RUN chmod u+w /usr/lib/jvm/java-11-openjdk/lib/*/*

# # Add server certificates to the default trust store
# COPY --from=downloader /tmp/global-bundle.pem /tmp/
# RUN keytool -importcert -file /tmp/global-bundle.pem \
#     -keystore /usr/lib/jvm/java-11-openjdk/lib/security/cacerts \
#     -storepass changeit -noprompt -alias rds-global-bundle

# Add your application jar
COPY target/athena-oracle-2022.47.1.jar ${LAMBDA_TASK_ROOT}
ENV JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/usr/lib/jvm/java-11-openjdk/lib/security/cacerts -Djavax.net.ssl.trustStorePassword=changeit"

# Unpack the jar
RUN jar xf athena-oracle-2022.47.1.jar
CMD [ "com.amazonaws.athena.connectors.oracle.OracleMuxCompositeHandler" ]
