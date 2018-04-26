package com.microsoft.azure.documentdb.changefeedprocessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import com.microsoft.azure.documentdb.*;
import java.util.logging.Logger;

class CollectionHelper {
	
	
	static long getDocumentCount(ResourceResponse<DocumentCollection> response) {
		Logger logger = Logger.getLogger(CollectionHelper.class.getName());
		assert response != null;
		String resourceUsage = response.getResponseHeaders().get("x-ms-resource-usage");
		if(resourceUsage != null) {
			String[] parts = resourceUsage.split(";");
			for(String part : parts) {
				String[] name = part.split("=");
				if(name.length>1 && name[0].equalsIgnoreCase("documentsCount") && !name[1].isEmpty() && name[1] != null) {
					long result = -1;
					try {
						result = Integer.parseInt(name[1]);
						return result;
					} catch (NumberFormatException e) {
						logger.warning(String.format("Failed to get document count from response, can't Int64.TryParse('{0}')", part));
					}
					
					break;
				}
			}
		}
		
		return -1;
	}
	
	static Callable<List<PartitionKeyRange>> enumPartitionKeyRangesAsync(DocumentClient client, String collectionSelfLink) {
		assert client != null;
		assert collectionSelfLink != null && !collectionSelfLink.isEmpty() && !collectionSelfLink.contains(" "); //Collection self link should not contain whitespaces
		Callable<List<PartitionKeyRange>> callable = new Callable<List<PartitionKeyRange>>() {
			@Override
			public List<PartitionKeyRange> call() {
				
				String partitionKeyRangesPath = String.format("{0}/pkranges", collectionSelfLink);
				FeedResponse<PartitionKeyRange> response = null;
		        List<PartitionKeyRange> partitionKeyRanges = new ArrayList<PartitionKeyRange>();
		        do
		        {
		            FeedOptions feedOptions = new FeedOptions(); 
		            feedOptions.setMaxBufferedItemCount(1000);
		            feedOptions.setRequestContinuation(response != null ? response.getResponseContinuation() : null);
		            response = client.readPartitionKeyRanges(partitionKeyRangesPath, feedOptions);
		            partitionKeyRanges.addAll(response.getQueryIterable().toList());
		        }
		        while (!response.getResponseContinuation().isEmpty() && response.getResponseContinuation() != null);

		        return partitionKeyRanges;
			}
		};
		return callable;
	}
}
