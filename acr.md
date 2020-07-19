### Create Datasette Web App in Azure

*An example of how to get started....*

Workflow centers on using the Azure bash cli, not the Azure Portal.

Before creating Azure all the neccessary resources, make sure you have a Docker image ready to be pushed to azure.

#### Steps to create Datasette Web App in Azure

First, create a Resource Group which will used to host App Service Plan and Web App.
```
az group create -n myResourceGroup -l "East US"
```

Create your App Service Plan within the newly created resource group.

```
az appservice plan create -n myAppServicePlan -g myResourceGroup --sku S1 --is-linux
```

Create your Azure Container Registry. 

```
az acr create -n myreg -g myResourceGroup --sku Basic --admin-enabled true
```

You can avoid going to the Portal to grab your credentials by running the following command. 

```
az acr credential show -n myreg
```

Login into the Azure Container Registry login server.

```
docker login docker.server.io -u user -p pw
```

Once authenticated, push Docker image to Azure Container Registry 

```
docker push docker.server.io/image:tag
```

Create Azure Web App. 

Use the ```-i``` flag to create a web app with an image from a private Azure Container Registry.

```
az webapp create -g myResourceGroup -p myAppServicePlan -n webApp -i docker.server.io/image:tag
```


Continuous deployment can be set up within Azure, emnabling automatic updates to the
the webapp when the docker image is pushed. This option is located under App Service > Container Settings > Azure Container Registry > Continuous Deployment.
