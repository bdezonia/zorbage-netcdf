/*
 * zorbage-netcdf: code for using the NetCDF data file library to open files into zorbage data structures for further processing
 *
 * Copyright (C) 2020 Barry DeZonia
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package nom.bdezonia.zorbage.netcdf;

import nom.bdezonia.zorbage.data.DimensionedDataSource;

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
		for (DimensionedDataSource<?> ds : data.floats) {
			dump(ds, "float");
		}
		for (DimensionedDataSource<?> ds : data.doubles) {
			dump(ds, "double");
		}
		for (String key : data.chars.keySet()) {
			System.out.println("CHARACTER DATA *******************************************");
			System.out.println("  key  = " + key);
			System.out.println("  text = " + data.chars.get(key));
		}
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
