package samples.processors.utils.callbacks;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;

public class XlsToCsvProcessorCallback implements StreamCallback {

	private static final String OUTPUT_DATE_FORMAT= "yyyy-MM-dd";
    private static final String CVS_SEPERATOR_CHAR=",";
    private static final String NEW_LINE_CHARACTER="\r\n";
    
	private int _sheetIndex = 0;
	private int _skipRows = 0;
	private int _takeRows = 0;
	private boolean _isSuccess = false;
	private ComponentLog _logger = null;
	
	public XlsToCsvProcessorCallback(int sheetIndex, int skipRows, int takeRows, ComponentLog logger) {
		this._sheetIndex = sheetIndex;
		this._skipRows = skipRows;
		this._takeRows = takeRows;
		this._logger = logger;
	}
	
	@Override
	public void process(InputStream in, OutputStream out) throws IOException {
		_logger.log(LogLevel.DEBUG, "Begin transformation to csv");
		try {
			HSSFWorkbook workBook = new HSSFWorkbook(in);
			HSSFSheet selectedSheet = workBook.getSheetAt(_sheetIndex);
			Iterator<Row> rowIterator = selectedSheet.rowIterator();
    		String csvData = "";
    		int currentRow = -1;
    		int indexToSkip = _skipRows - 1;
    		try {
    			while(rowIterator.hasNext()) {
        			HSSFRow row = (HSSFRow)rowIterator.next();
        			
        			currentRow = currentRow + 1;
        			if(currentRow <= _takeRows) {
	        			if(currentRow > indexToSkip) {
	        			
		        			for(int i = 0; i < row.getLastCellNum(); i++ ) {
		        				csvData += getCellData(row.getCell(i));
		        			}
		        			
		        			csvData = csvData.substring(0, csvData.length() - 2);
		        			
		        			if(rowIterator.hasNext() || currentRow < _takeRows) {
		        				csvData += NEW_LINE_CHARACTER;
		        			}
	        			}
        			}
        		}
    			Writer streamWriter = new OutputStreamWriter(out,"UTF-8");
    			streamWriter.write(csvData);
    			streamWriter.close();
    			workBook.close();
    			_isSuccess = true;
    		}
    		catch (Exception e) {
    			_logger.error(e.getMessage());
			}
		}
		catch (Exception e) {
			_logger.error(e.getMessage());
		}
	}
	
	public boolean SuccessOnProcessing() {
		return _isSuccess;
	}
	
	private String getCellData(HSSFCell cell) throws Exception {
		String cellData = "";
		if ( cell== null) {
            cellData += CVS_SEPERATOR_CHAR;
        }
		else {
            switch(cell.getCellType() ){
                case  STRING :
                case  BOOLEAN :
                         cellData +=  cell.getRichStringCellValue ()+CVS_SEPERATOR_CHAR;
                         break;
                case NUMERIC :
                        cellData += getNumericValue(cell);
                        break;
                case  FORMULA:
                        cellData +=  getFormulaValue(cell);
            default:
                cellData += CVS_SEPERATOR_CHAR;;
            }
        }
		return cellData;
	}
	
	/**
     * Get the formula value from a cell
     * @param myCell
     * @return
     * @throws Exception
     */
    private static String getFormulaValue(HSSFCell myCell) throws Exception{
        String cellData="";
         if ( myCell.getCachedFormulaResultType() == CellType.STRING  || myCell.getCellType () == CellType.BOOLEAN) {
             cellData +=  myCell.getRichStringCellValue ()+CVS_SEPERATOR_CHAR;
         }else  if ( myCell.getCachedFormulaResultType() == CellType.NUMERIC ) {
             cellData += getNumericValue(myCell)+CVS_SEPERATOR_CHAR;
         }
         return cellData;
    }
    /**
     * Get the date or number value from a cell
     * @param myCell
     * @return
     * @throws Exception
     */
    private static String getNumericValue(HSSFCell myCell) throws Exception {
        String cellData="";
         if ( HSSFDateUtil.isCellDateFormatted(myCell) ){
               cellData += new SimpleDateFormat(OUTPUT_DATE_FORMAT).format(myCell.getDateCellValue()) +CVS_SEPERATOR_CHAR;
           }else{
               cellData += new BigDecimal(myCell.getNumericCellValue()).toString()+CVS_SEPERATOR_CHAR ;
           }
        return cellData;
    }

}
