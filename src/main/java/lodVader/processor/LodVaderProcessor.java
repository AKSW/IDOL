/**
 * 
 */
package lodVader.processor;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 11, 2016
 */
public class LodVaderProcessor {

//	final static Logger logger = LoggerFactory.getLogger(LodVaderProcessor.class);
//
//	/**
//	 * Process a dataset and its distributions. 
//	 * @param dataset: the datasetDB instance
//	 * @param streamFile: the dataset streamer processor
//	 * @throws Exception
//	 */
//	public void datasetProcessor(DatasetDB dataset, LodVaderStreamProcessor streamFile) throws Exception {
//
//		// iterate on distributions of current dataset
//		for (DistributionDB distribution : dataset.getDistributionsAsMongoDBObjects()) {
//
//			logger.info("Processing distribution: " + distribution.getDownloadUrl());
//
//			distribution.setStatus(DistributionStatus.WAITING_TO_STREAM);
//
//			// check is distribution need to be streamed
//			boolean needDownload = checkDistributionStatus(distribution);
//			// boolean needDownload = true;
//
//			if (!needDownload) {
//				logger.info("Distribution is already in the last version. No needs to stream again. ");
//				distribution.setLastMsg("Distribution is already in the last version. No needs to stream again.");
//				distribution.update(true);
//			}
//
//			// if distribution have not already been handled
//			if (needDownload)
//				try {
//
//					// uptate status of distribution to streaming
//					distribution.setStatus(DistributionStatus.STREAMING);
//					distribution.update(true);
//
//					logger.info("Streaming distribution.");
//
//					streamFile.streamDistribution(distribution);
//
//					// uptate status of distribution
//					distribution.find(true);
//					distribution.setStatus(DistributionStatus.STREAMED);
//					distribution.update(true);
//
//					logger.debug("Distribution streamed. ");
//
//					logger.debug("Saving mongodb \"Distribution\" document.");
//
//					distribution.setNumberOfObjectTriples(streamFile.getTupleManager().getObjectLines());
//					distribution.setNumberOfSubjectTriples(streamFile.getTupleManager().getSubjectLines());
//					distribution.setDownloadUrl(streamFile.downloadUrl.toString());
//					distribution.setFormat(streamFile.extension.toString());
//					distribution.setHttpByteSize(String.valueOf((int) streamFile.httpContentLength));
//					distribution.setHttpFormat(streamFile.httpContentType);
//					distribution.setHttpLastModified(streamFile.httpLastModified);
//					distribution.setTriples(streamFile.getTupleManager().getTotalTriples());
//
//					distribution.setSuccessfullyDownloaded(true);
//					distribution.update(true);
//
//
//					logger.debug("Done streaming mongodb distribution object.");
//
//					// uptate status of distribution
//					 distribution.setStatus(DistributionStatus.DONE);
////					distribution.setStatus(DistributionStatus.WAITING_TO_STREAM);
//
//					DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss dd/MM/yyyy");
//					// get current date time with Date()
//					Date date = new Date();
//
//					distribution.setLastTimeStreamed(dateFormat.format(date).toString());
//
//					distribution.update(true);
//
//					logger.info("Distribution " + distribution.getDownloadUrl() + " processed! ");
//					logger.info(" = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
//					logger.info(" = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
//
//				} catch (Exception e) {
//					// uptate status of distribution
//					distribution.setStatus(DistributionStatus.ERROR);
//					distribution.setLastMsg(e.getMessage());
//
//					e.printStackTrace();
//					distribution.setSuccessfullyDownloaded(false);
//					distribution.update(true);
//
//				}
//		}
//	}
//
//	private boolean checkDistributionStatus(DistributionDB distributionMongoDBObj) throws Exception {
//		boolean needDownload = false;
//
//		if (distributionMongoDBObj.getStatus().equals(DistributionStatus.WAITING_TO_STREAM))
//			needDownload = true;
//		else if (distributionMongoDBObj.getStatus().equals(DistributionStatus.STREAMING))
//			needDownload = false;
//		else if (distributionMongoDBObj.getStatus().equals(DistributionStatus.ERROR))
//			needDownload = true;
//		else if (new CheckWhetherToStream().checkDistribution(distributionMongoDBObj))
//			needDownload = true;
//
//		return needDownload;
//	}

}
