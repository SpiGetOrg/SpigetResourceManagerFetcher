package org.spiget.resourcemanagerfetcher;

public class Main {

	public static void main(String...args) throws Exception {
		SpigetRestFetcher fetcher = new SpigetRestFetcher();
		if (fetcher.init() == null) {
			System.exit(-1);
			return;
		}
		fetcher.fetch();
	}

}
