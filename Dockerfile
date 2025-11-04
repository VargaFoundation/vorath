FROM ghcr.io/graalvm/native-image:ol8-java17 as build

WORKDIR /app
COPY target/kubernetes-csi-1.0.0-SNAPSHOT.jar kubernetes-csi.jar

RUN gu install native-image
RUN native-image -jar kubernetes-csi.jar --no-fallback -H:Name=application

FROM oraclelinux:8-slim
COPY --from=build /app/application /app/application
CMD ["/app/application"]