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

import nom.bdezonia.zorbage.multidim.MultiDimDataSource;
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
	public static void main(String[] args)
	{
		//if (args.length != 1) {
		//	System.out.println("Usage: <appname> <netcdf input filename>");
		//	System.exit(0);
		//}
		//String filename = args[0];
		String filename = "/home/bdz/images/qesdi/cru_v3_dtr_clim10.nc";
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
			for (MultiDimDataSource<?> ds : data.int1s) {
				System.out.println("DS uint1");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.int8s) {
				System.out.println("DS int8");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.uint8s) {
				System.out.println("DS uint8");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.int16s) {
				System.out.println("DS int16");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.uint16s) {
				System.out.println("DS uint16");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.int32s) {
				System.out.println("DS int32");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.uint32s) {
				System.out.println("DS uint32");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.int64s) {
				System.out.println("DS int64");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.uint64s) {
				System.out.println("DS uint64");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.floats) {
				System.out.println("DS float");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
			}
			for (MultiDimDataSource<?> ds : data.doubles) {
				System.out.println("DS double");
				System.out.print("  ");
				for (int i = 0; i < ds.numDimensions(); i++) {
					if (i != 0)
						System.out.print(",");
					System.out.print(ds.dimension(i));
				}
				System.out.println();
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
	
}
