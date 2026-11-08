# Build context: ./dotnet  (docker build -f MockConsumer/Dockerfile .)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props ./
COPY Shared/Shared.csproj Shared/
COPY MockConsumer/MockConsumer.csproj MockConsumer/
RUN dotnet restore MockConsumer/MockConsumer.csproj
COPY Shared/ Shared/
COPY MockConsumer/ MockConsumer/
RUN dotnet publish MockConsumer/MockConsumer.csproj -c Release -o /out --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /out .
USER $APP_UID
# HTTP_PORTS is cleared because the app binds via METRICS_ADDR instead.
ENV DOTNET_gcServer=0 \
    ASPNETCORE_HTTP_PORTS= \
    Logging__Console__FormatterName=simple \
    Logging__Console__FormatterOptions__SingleLine=true \
    Logging__Console__FormatterOptions__TimestampFormat="yyyy/MM/dd HH:mm:ss " \
    Logging__LogLevel__Microsoft.AspNetCore=Warning
EXPOSE 2114
ENTRYPOINT ["dotnet", "MockConsumer.dll"]
