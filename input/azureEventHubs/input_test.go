package azureEventHubs

import (
	"context"
	"fmt"
	"github.com/Azure/azure-amqp-common-go/v3/conn"
	"github.com/Azure/azure-amqp-common-go/v3/sas"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/eph"
	"github.com/Azure/azure-event-hubs-go/v3/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	connStr := "Endpoint=sb://anhntdemo.servicebus.windows.net/;SharedAccessKeyName=full;SharedAccessKey=MiSzVLb46ZDiG55rhtDxdiCfDV0O5mVtT+AEhGOeVoo=;EntityPath=hubs_demo"
	hub, err := eventhub.NewHubFromConnectionString(connStr)

	if err != nil {
		fmt.Println(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// send a single message into a random partition
	err = hub.Send(ctx, eventhub.NewEventFromString("JOSN JOSN JSON JSON"))
	if err != nil {
		fmt.Println(err)
		return
	}
	
}

func TestReceiver(t *testing.T) {
	storageAccountName := "anhntstorage"
	storageAccountKey := "fS+fNK6QaKIeY3yddLDtIFSJiwF2OiOmIqCyaII+sMSY9I+90ONEVCGmQB2nrV6w3jdluhlYtWlG+AStZKuzaA=="
	// Azure Storage container to store leases and checkpoints
	storageContainerName := "demo"

	// Azure Event Hub connection string
	eventHubConnStr := "Endpoint=sb://anhntdemo.servicebus.windows.net/;SharedAccessKeyName=full;SharedAccessKey=MiSzVLb46ZDiG55rhtDxdiCfDV0O5mVtT+AEhGOeVoo=;EntityPath=hubs_demo"
	parsed, err := conn.ParsedConnectionFromStr(eventHubConnStr)
	if err != nil {
		fmt.Println(err)
	}

	// create a new Azure Storage Leaser / Checkpointer
	cred, err := azblob.NewSharedKeyCredential(storageAccountName, storageAccountKey)
	if err != nil {
		fmt.Println(err)
		return
	}

	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(cred, storageAccountName, storageContainerName, azure.PublicCloud)
	if err != nil {
		fmt.Println(err)
		return
	}

	// SAS token provider for Azure Event Hubs
	provider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(parsed.KeyName, parsed.Key))
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// create a new EPH processor
	processor, err := eph.New(ctx, parsed.Namespace, parsed.HubName,"http://192.168.5.8:3128", provider, leaserCheckpointer, leaserCheckpointer)
	if err != nil {
		fmt.Println(err)
		return
	}

	// register a message handler -- many can be registered
	handlerID, err := processor.RegisterHandler(ctx,
		func(c context.Context, e *eventhub.Event) error {
			fmt.Println(string(e.Data))
			return nil
		})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("handler id: %q is running\n", handlerID)

	// unregister a handler to stop that handler from receiving events
	// processor.UnregisterHandler(ctx, handleID)

	// start handling messages from all of the partitions balancing across multiple consumers
	err = processor.Start(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

}

var environments = map[string]azure.Environment{
	azure.ChinaCloud.ResourceManagerEndpoint:        azure.ChinaCloud,
	azure.GermanCloud.ResourceManagerEndpoint:       azure.GermanCloud,
	azure.PublicCloud.ResourceManagerEndpoint:       azure.PublicCloud,
	azure.USGovernmentCloud.ResourceManagerEndpoint: azure.USGovernmentCloud,
}

func TestStart(t *testing.T) {
	storageAccountName := "anhntstorage"
	storageAccountKey := "fS+fNK6QaKIeY3yddLDtIFSJiwF2OiOmIqCyaII+sMSY9I+90ONEVCGmQB2nrV6w3jdluhlYtWlG+AStZKuzaA=="
	// Azure Storage container to store leases and checkpoints
	storageContainerName := "demo"

	// Azure Event Hub connection string
	eventHubConnStr := "Endpoint=sb://anhntdemo.servicebus.windows.net/;SharedAccessKeyName=full;SharedAccessKey=MiSzVLb46ZDiG55rhtDxdiCfDV0O5mVtT+AEhGOeVoo=;EntityPath=hubs_demo"
	parsed, err := conn.ParsedConnectionFromStr(eventHubConnStr)
	if err != nil {
		// handle error
	}

	// create a new Azure Storage Leaser / Checkpointer
	cred, err := azblob.NewSharedKeyCredential(storageAccountName, storageAccountKey)
	if err != nil {
		fmt.Println(err)
		return
	}

	leaserCheckpointer, err := storage.NewStorageLeaserCheckpointer(cred, storageAccountName, storageContainerName, azure.PublicCloud)
	if err != nil {
		fmt.Println(err)
		return
	}

	// SAS token provider for Azure Event Hubs
	provider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(parsed.KeyName, parsed.Key))
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// create a new EPH processor
	processor, err := eph.New(ctx, parsed.Namespace, parsed.HubName,"http://192.168.5.8:3128", provider, leaserCheckpointer, leaserCheckpointer)
	if err != nil {
		fmt.Println(err)
		return
	}

	// register a message handler -- many can be registered
	handlerID, err := processor.RegisterHandler(ctx,
		func(c context.Context, e *eventhub.Event) error {
			fmt.Println(string(e.Data))
			return nil
		})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("handler id: %q is running\n", handlerID)

	// unregister a handler to stop that handler from receiving events
	// processor.UnregisterHandler(ctx, handleID)

	// start handling messages from all of the partitions balancing across multiple consumers
	err = processor.Start(ctx)
	if err != nil {
		fmt.Println(err)
	}
}