# Copyright (c) HyperCloud Development Team.
# Distributed under the terms of the Modified BSD License.

# Global Arguments
ARG build_image=gradle:6.0.1-jdk8
ARG base_image=openjdk:8-jdk

FROM $build_image AS build

LABEL maintainer="Junxiang Wei <kevinprotoss.wei@gmail.com>"

COPY --chown=gradle:gradle . /home/gradle/workspace
WORKDIR /home/gradle/workspace
RUN gradle build bootJar --no-daemon

FROM $base_image

ARG version=0.0.3

COPY --from=build /home/gradle/workspace/build/distributions/* /tmp/
RUN unzip /tmp/datastream-writer-boot-$version.zip
RUN mkdir /app
RUN mv datastream-writer-boot-$version/lib/datastream-writer-$version.jar /app/datastream-writer.jar

ENTRYPOINT ["java", "-jar", "/app/datastream-writer.jar"]