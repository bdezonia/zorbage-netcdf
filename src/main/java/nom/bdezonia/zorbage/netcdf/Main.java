/*
 * zorbage-netcdf: code for loading netcdf files into zorbage data structures for further processing
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import nom.bdezonia.zorbage.data.DimensionedDataSource;
import ucar.ma2.Array;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

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
	static void test()
	{
		//if (args.length != 1) {
		//	System.out.println("Usage: <appname> <netcdf input filename>");
		//	System.exit(0);
		//}
		//String filename = args[0];
		//String filename = "/home/bdz/images/qesdi/cru_v3_dtr_clim10.nc";
		String filename = "/home/bdz/images/qesdi/wwf_olson2006_ecosystems.nc";
		NetcdfFile ncfile = null;
		try {
			ncfile = NetcdfFiles.open(filename);
			List<Dimension> dims = ncfile.getDimensions();
			System.out.println("file info: dims = "+dims.size());
			for (int i = 0; i < dims.size(); i++) {
				System.out.println("  "+dims.get(i).getLength());
			}
			List<Variable> bands = ncfile.getVariables();
			System.out.println("band info: count = "+bands.size());
			for (int i = 0; i < bands.size(); i++) {
				Variable band = bands.get(i);
				System.out.println("  band " + i);
				System.out.println("    type " + band.getDataType());
				System.out.println("    desc " + band.getDescription());
				System.out.println("    loc  " + band.getDatasetLocation());
				System.out.println("    dims " + band.getDimensionsString());
				System.out.println("    rank " + band.getRank());
				System.out.println("    shap " + Arrays.toString(band.getShape()));
				System.out.println("    size " + band.getSize());
				System.out.println("    unit " + band.getUnitsString());
				if (band.getDataType().toString().equals("char")) {
					Array arr = band.read();
					for (int j = 0; j < band.getSize(); j++) {
						if (j % 80 == 0)
							System.out.println();
						System.out.print(arr.getChar(j));
					}
					System.out.println();
				}
			}
			DataBundle data = NetCDF.loadData(filename);
			for (DimensionedDataSource<?> ds : data.int1s) {
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
		} catch (IOException e) {
			System.out.println("error trying to open " + filename);
		};
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
