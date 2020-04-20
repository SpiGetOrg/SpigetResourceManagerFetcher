package org.spiget.resourcemanagerfetcher;

public class Main {

	public static void main(String...args) throws Exception {
		SpigetRestFetcher fetcher = new SpigetRestFetcher();
		fetcher.init();
		fetcher.fetch();
	}

}
