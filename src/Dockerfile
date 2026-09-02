FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["AppViewLite.Web/AppViewLite.Web.csproj", "AppViewLite.Web/"]
COPY ["AppViewLite/AppViewLite.csproj", "AppViewLite/"]
RUN dotnet restore "AppViewLite.Web/AppViewLite.Web.csproj"

COPY AppViewLite.Web/ AppViewLite.Web/
COPY AppViewLite/ AppViewLite/

RUN dotnet publish "AppViewLite.Web/AppViewLite.Web.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .

ENV APPVIEWLITE_DIRECTORY=/data
ENV ASPNETCORE_ENVIRONMENT=Production
ENV APPVIEWLITE_BIND_URLS=http://+:8080
ENV APPVIEWLITE_ALLOW_NEW_DATABASE=1
ENV APPVIEWLITE_CONFIGURATION=/data/configuration/appviewlite.env
ENV APPVIEWLITE_BLOCKLIST_PATH=/data/configuration/appviewlite-blocklist.ini

EXPOSE 8080

VOLUME $APPVIEWLITE_DIRECTORY

ENTRYPOINT ["dotnet", "AppViewLite.Web.dll"]