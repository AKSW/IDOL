package org.aksw.idol.API.diagram;

import java.util.ArrayList;
import java.util.HashMap;

public class Diagram {
	
	ArrayList<Link> links = new ArrayList<Link>();

	public HashMap<String, Node> nodes = new HashMap<String, Node>();

	public Node addNode(Node source) {
		if (!nodes.containsKey(source.getID())) {
			nodes.put(source.getID(), source);
			return source; 
		} else {
			Node b = nodes.get(source.getID());
			return b;
		}
	}

	public void addLink(Link link) {
		if (link.nodeSource.url.equals(link.nodeTarget.url))
			return;
		for (Link l : links) {
			if (l.nodeSource.url.equals(link.nodeSource.url)
					&& l.nodeTarget.url.equals(link.nodeTarget.url))
				return;
		}
		links.add(link);
	} 

	public void printSelectedBubbles(String[] b1) {
		for (Node bubble : nodes.values()) {
			for (String b : b1) {
				if (bubble.getID() ==  b) {
					bubble.setColor("rgb(189, 189, 189)");
				}
			}
		}
	}

	public boolean checkIfBubbleExists(String bubbleURI) {
		if (nodes.containsKey(bubbleURI))
			return true;
		else
			return false;
	}

	public ArrayList<Link> getLinks() {
		return links;
	}

	public void setLinks(ArrayList<Link> links) {
		this.links = links;
	}

	public ArrayList<Node> getNodes() {
		return new ArrayList<Node>(nodes.values());
	}

}
