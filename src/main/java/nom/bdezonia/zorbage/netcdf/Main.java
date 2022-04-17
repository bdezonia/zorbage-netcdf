/*
 * zorbage-netcdf: code for using the NetCDF data file library to open files into zorbage data structures for further processing
 *
 * Copyright (C) 2020-2021 Barry DeZonia
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package nom.bdezonia.zorbage.netcdf;

import nom.bdezonia.zorbage.data.DimensionedDataSource;
//import nom.bdezonia.zorbage.misc.
import nom.bdezonia.zorbage.misc.DataBundle;

/**
 * 
 * @author Barry DeZonia
 *
 */
public class Main
{
	/**
	 * 
	 * @param args
	 */
	/* public */ static void main(String[] args)
	{
		//if (args.length != 1) {
		//	System.out.println("Usage: <appname> <netcdf input filename>");
		//	System.exit(0);
		//}
		//String filename = args[0];
		//String filename = "/home/bdz/images/qesdi/cru_v3_dtr_clim10.nc";
		String filename = "/home/bdz/images/qesdi/wwf_olson2006_ecosystems.nc";
		DataBundle data = NetCDF.loadAllDatasets(filename);
		for (DimensionedDataSource<?> ds : data.uint1s) {
			dump(ds, "uint1");
		}
		for (DimensionedDataSource<?> ds : data.int8s) {
			dump(ds, "int8");
		}
		for (DimensionedDataSource<?> ds : data.uint8s) {
			dump(ds, "uint8");
		}
		for (DimensionedDataSource<?> ds : data.int16s) {
			dump(ds, "int16");
		}
		for (DimensionedDataSource<?> ds : data.uint16s) {
			dump(ds, "uint16");
		}
		for (DimensionedDataSource<?> ds : data.int32s) {
			dump(ds, "int32");
		}
		for (DimensionedDataSource<?> ds : data.uint32s) {
			dump(ds, "uint32");
		}
		for (DimensionedDataSource<?> ds : data.int64s) {
			dump(ds, "int64");
		}
		for (DimensionedDataSource<?> ds : data.uint64s) {
			dump(ds, "uint64");
		}
		for (DimensionedDataSource<?> ds : data.flts) {
			dump(ds, "float");
		}
		for (DimensionedDataSource<?> ds : data.dbls) {
			dump(ds, "double");
		}
		for (DimensionedDataSource<?> ds : data.fstrs) {
			dump(ds, "fstrings");
		}
		for (DimensionedDataSource<?> ds : data.chars) {
			dump(ds, "char");
		}
		/*
		for (String key : data.chars.keySet()) {
			System.out.println("CHARACTER DATA *******************************************");
			System.out.println("  key  = " + key);
			System.out.println("  text = " + data.chars.get(key));
		}
		*/
	}
	
	private static void dump(DimensionedDataSource<?> ds, String type) {
		System.out.println("Dataset created of type " + type);
		System.out.print("  dims = [");
		for (int i = 0; i < ds.numDimensions(); i++) {
			if (i != 0)
				System.out.print(",");
			System.out.print(ds.dimension(i));
		}
		System.out.println("]");
	}
	
}
