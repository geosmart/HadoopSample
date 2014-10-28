<b>Use maven module mange project because it's easy to clarify every module's dependences</b>

<b>multithreaddownload:</b>
	provide download file from url and save to specified file
	<i>"SpecifiedUrlDownloadRunnable"</i> class provide a functionality of download and save to file ,this is a basic function for download, It implement Runnable interface
	<i>"DownloadUrlFileToLocalFileNoTempfile"</i> A business logical manager class is responsible for creating and managing each uri's download thread. first create thread based on specified thread number
	<i>DownloadUrlFileToLocalHaveTempFile</i> A business logical manager class is responsible for creating and managing each uri's download thread. It's not same with "DownloadUrlFileToLocalFileNoTempfile"
	 this class object first create specified nuber's temp file and then breakdown uri's stream  into parts that's number is equal to thread number, save to temp file. finally merge the temp file to specified file