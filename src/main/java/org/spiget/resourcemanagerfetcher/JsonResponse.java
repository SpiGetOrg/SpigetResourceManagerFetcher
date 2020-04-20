package org.spiget.resourcemanagerfetcher;

import com.google.gson.JsonElement;

public class JsonResponse {

	int code;
	JsonElement json;

	public JsonResponse(int code, JsonElement json) {
		this.code = code;
		this.json = json;
	}
}
