package org.spiget.resourcemanagerfetcher;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.bson.Document;
import org.jetbrains.annotations.Nullable;
import org.spiget.client.json.JsonClient;
import org.spiget.client.json.JsonResponse;
import org.spiget.data.UpdateRequest;
import org.spiget.data.resource.Rating;
import org.spiget.data.resource.Resource;
import org.spiget.database.DatabaseClient;
import org.spiget.database.DatabaseParser;
import org.spiget.database.SpigetGson;

import java.io.FileReader;
import java.io.IOException;

@Log4j2
public class SpigetRestFetcher {

	public static JsonObject config;

	public static DatabaseClient databaseClient;

	int  itemsPerFetch = 500;
	long delay         = 1000;
	int start =0;

	@Nullable
	public SpigetRestFetcher init() throws IOException {
		config = new JsonParser().parse(new FileReader("config.json")).getAsJsonObject();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					log.info("Disconnecting database...");
					databaseClient.disconnect();
				} catch (IOException e) {
					log.warn("Failed to disconnect from database", e);
				}
			}
		});

		{
			log.info("Initializing & testing database connection...");
			long testStart = System.currentTimeMillis();
			try {
				databaseClient = new DatabaseClient(
						config.get("database.name").getAsString(),
						config.get("database.host").getAsString(),
						config.get("database.port").getAsInt(),
						config.get("database.user").getAsString(),
						config.get("database.pass").getAsString().toCharArray(),
						config.get("database.db").getAsString());
				databaseClient.connect(config.get("database.timeout").getAsInt());
				databaseClient.collectionCount();
				log.info("Connection successful (" + (System.currentTimeMillis() - testStart) + "ms)");
			} catch (Exception e) {
				log.fatal("Connection failed after " + (System.currentTimeMillis() - testStart) + "ms", e);
				log.fatal("Aborting.");

				System.exit(-1);
				return null;
			}
		}

		{
			log.info("Testing SpigotMC connection...");
			long testStart = System.currentTimeMillis();
			try {
				JsonResponse response = JsonClient.get("https://api.spigotmc.org/simple/0.1/index.php?action=getResource&id=2");
				if (response == null) { return null; }
				int code = response.code;
				if (code >= 200 && code < 400) {
					log.info("Connection successful (" + (System.currentTimeMillis() - testStart) + "ms)");
				} else {
					log.fatal("Connection failed with code " + code + " after " + (System.currentTimeMillis() - testStart) + "ms");
					log.fatal("Aborting.");
					log.info(response.json);

					System.exit(-1);
					return null;
				}
			} catch (Exception e) {
				log.fatal("Connection failed after " + (System.currentTimeMillis() - testStart) + "ms", e);
				log.fatal("Aborting.");

				System.exit(-1);
				return null;
			}
		}

		itemsPerFetch = config.get("database.itemsPerFind").getAsInt();
		JsonClient.userAgent = config.get("request.userAgent").getAsString();
		JsonClient.logConn = config.get("debug.connections").getAsBoolean();
		delay = config.get("fetch.pause").getAsLong();
		start = config.get("fetch.start").getAsInt();

		return this;
	}

	public void fetch() {
		int n = start;
		int c = 0;
		while ((c = fetchN(n)) >= itemsPerFetch) {
			n++;
		}
	}

	public int fetchN(int n) {
		log.info("Running Fetch #" + n);

		FindIterable<Document> iterable = databaseClient.getResourcesCollection().find().sort(new Document("updateDate", 1)).limit(itemsPerFetch).skip(n * itemsPerFetch);
		int c = 0;
		long updateStart = System.currentTimeMillis();

		for (Document document : iterable) {
			c++;

			log.info("F" + n + " I" + c);

			try {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Resource resource = SpigetGson.RESOURCE.fromJson(DatabaseParser.toJson(document), Resource.class);
				JsonResponse response = JsonClient.get("https://api.spigotmc.org/simple/0.1/index.php?action=getResource&id=" + resource.getId());
				if (response == null) { continue; }
				if (response.code != 200) {
					log.warn("Got Code " + response.code + " for getResource #" + resource.getId());
					if (response.code == 503) {// Cloudflare
					} else if (response.code == 404) {// Resource not found
						log.info("Scheduling #" + resource.getId() + " for deletion");
						requestUpdate(resource.getId(), true);
					} else {
						log.error("Unexpected status code");
					}
				} else {
					if (!response.json.isJsonObject()) {
						log.warn("Expected response to be a json object but was not.");
						log.warn(response.json);
					} else {
						JsonObject json = response.json.getAsJsonObject();

						String title = json.get("title").getAsString();
						if (resource.getName() != null && title != null && !resource.getName().equals(title)) {// name changed
							log.info("Name of #" + resource.getId() + " changed  \"" + resource.getName() + "\" -> \"" + title + "\"");
							updateName(resource.getId(), title);
						}

						String tag = json.get("tag").getAsString();
						if (resource.getTag() != null && tag != null && !resource.getTag().equals(tag)) {// tag changed
							log.info("Tag of #" + resource.getId() + " changed  \"" + resource.getTag() + "\" -> \"" + tag + "\"");
							updateTag(resource.getId(), tag);
						}

						boolean requestUpdate = false;

						JsonObject statsJson = json.get("stats").getAsJsonObject();
						if (statsJson != null) {
							int downloads = statsJson.get("downloads").getAsInt();
							if (resource.getDownloads() != downloads) {
								log.info("Downloads of #" + resource.getId() + " changed  " + resource.getDownloads() + " -> " + downloads);
								updateDownloads(resource.getId(), downloads);
							}

							Rating rating = resource.getRating();
							if (rating != null) {
								int ratingCount = statsJson.get("reviews").getAsInt();
								float ratingAvg = statsJson.get("rating").getAsFloat();
								if (ratingCount > rating.getCount()) {
//									requestUpdate = true;
									updateRatingCount(resource.getId(), ratingCount);
								}
								if (ratingAvg != rating.getAverage()) {
//									requestUpdate = true;
									updateRatingAvg(resource.getId(), ratingAvg);
								}
							}

							int updates = statsJson.get("updates").getAsInt();
							if (updates > resource.getUpdates().size()) {
//								requestUpdate = true;
							}
						}

						if (requestUpdate) {
							log.info("Requesting update for #" + resource.getId());
							requestUpdate(resource.getId(), false);
						}

					}
				}
			} catch (Exception e) {
				log.log(Level.WARN, "Exception while trying to check resource", e);
			}
		}

		log.log(Level.INFO, "Finished fetch #"+n+". Took " + (((double) System.currentTimeMillis() - updateStart) / 1000.0 / 60.0) + " minutes to update " + c + " resources.");

		return c;
	}

	// From  https://github.com/SpiGetOrg/SpigetExistence/blob/master/src/main/java/org/spiget/existence/SpigetExistence.java#L201

	void setResourceStatus(int id, int status) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("existenceStatus", status)));
	}

	void updateName(int id, String name) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("name", name).append("fetch.restLatest", System.currentTimeMillis())));
	}

	void updateTag(int id, String tag) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("tag", tag).append("fetch.restLatest", System.currentTimeMillis())));
	}

	void updateDownloads(int id, int downloads) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("downloads", downloads).append("fetch.restLatest", System.currentTimeMillis())));
	}

	void updateRatingCount(int id, int count) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("rating.count", count).append("fetch.restLatest", System.currentTimeMillis())));
	}

	void updateRatingAvg(int id, float rating) {
		databaseClient.getResourcesCollection().updateOne(new Document("_id", id), new Document("$set", new Document("rating.average", rating).append("fetch.restLatest", System.currentTimeMillis())));
	}

	void requestUpdate(int id) {
		requestUpdate(id, false);
	}

	void requestUpdate(int id, boolean shouldDelete) {
		requestUpdate(id, shouldDelete, true, true, true);
	}

	void requestUpdate(int id, boolean shouldDelete, boolean versions, boolean updates, boolean reviews) {
		UpdateRequest request = new UpdateRequest();
		request.setDelete(shouldDelete);
		request.setVersions(versions);
		request.setUpdates(updates);
		request.setReviews(reviews);
		request.setRequestedId(id);
		request.setRequested(System.currentTimeMillis());

		Document document = DatabaseParser.toDocument(SpigetGson.UPDATE_REQUEST.toJsonTree(request));
		databaseClient.getUpdateRequestsCollection().insertOne(document);
	}

}
