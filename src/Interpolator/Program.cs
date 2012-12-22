using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Interpolator
{
    class Program
    {

        static readonly ConcurrentBag<Tuple<long,long>> _NewValues = new ConcurrentBag<Tuple<long, long>>();

        static void Main(string[] args)
        {
            var connStr = ConfigurationManager.ConnectionStrings[args[0]].ConnectionString;
            var xColumn = args[1];
            var yColumn = args[2];
            var tableName = args[3];
            
            const int windowSize = 27;
            Debug.Assert(windowSize % 2 == 1, "window size must be odd");

            var keyQueue = new Queue<Tuple<long,long>>(windowSize);
            var currentPosition = Int64.MinValue;
            using (var command = new SqlCommand(string.Format("SELECT {0},{1} FROM {2}", xColumn, yColumn, tableName), new SqlConnection(connStr)))
            {
                command.Connection.Open();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var newKeyValue = reader.GetInt64(0);
                        var newDataValue = reader.GetInt64(1);
                        keyQueue.Enqueue(Tuple.Create(newKeyValue, newDataValue));

                        if (currentPosition == Int64.MinValue)
                        {
                            currentPosition = newKeyValue;
                        }
                        else
                        {
                            while (currentPosition != newKeyValue)
                            {
                                _Interpolate(keyQueue, currentPosition);
                                currentPosition++;
                            }
                        }

                        currentPosition++;

                        if (keyQueue.Count < windowSize)
                            continue;
                        
                        keyQueue.Dequeue();
                    }
                }
                command.Connection.Close();
            }

            var finalNewValues = _NewValues
                .GroupBy(x => x.Item1)
                .Select(x => new
                                 {
                                     XValue = x.Key, 
                                     Estimate = Convert.ToInt64(Math.Round(x.Average(a => a.Item2)))
                                 }).OrderBy(x => x.XValue).ToArray();

            foreach (var finalNewValue in finalNewValues)
            {
                using (var conn = new SqlConnection(connStr))
                {
                    var commandString = string.Format("INSERT INTO [{0} ([{1}],[{2}]) VALUES ({3},{4})",tableName, xColumn, yColumn, finalNewValue.XValue, finalNewValue.Estimate);
                    conn.Open();
                    using (var cmd = new SqlCommand(commandString, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conn.Close();
                }
            }
            
        }

        private static void _Interpolate(Queue<Tuple<long, long>> keyQueue, long missingX)
        {
            var window = keyQueue.ToArray();

            double rsquared;
            double ylongercept;
            double slope;
            LinearRegression(window.Select(x => Convert.ToDouble(x.Item1)).ToArray(), window.Select(y => Convert.ToDouble(y.Item2)).ToArray(), out rsquared, out ylongercept, out slope);

            var missingEstimate = Convert.ToInt64(Math.Round((missingX * slope) + ylongercept));

            var newTuple = Tuple.Create(missingX, missingEstimate);
            _NewValues.Add(newTuple);
        }

        /// <summary>
        /// Fits a line to a collection of (x,y) polongs.
        /// </summary>
        /// <param name="xVals">The x-axis values.</param>
        /// <param name="yVals">The y-axis values.</param>
        /// <param name="inclusiveStart">The inclusive inclusiveStart index.</param>
        /// <param name="exclusiveEnd">The exclusive exclusiveEnd index.</param>
        /// <param name="rsquared">The r^2 value of the line.</param>
        /// <param name="ylongercept">The y-longercept value of the line (i.e. y = ax + b, ylongercept is b).</param>
        /// <param name="slope">The slop of the line (i.e. y = ax + b, slope is a).</param>
        public static void LinearRegression(double[] xVals, double[] yVals,
                                            out double rsquared, out double ylongercept,
                                            out double slope)
        {
            Debug.Assert(xVals.Length == yVals.Length);

            double sumOfX = xVals.Sum();
            double sumOfY = yVals.Sum();
            double sumOfXSq = xVals.Select(x => x * x).Sum();
            double sumOfYSq = yVals.Select(y => y*y).Sum();
            double sumCodeviates = xVals.Select((x, idx) => x*yVals[idx]).Sum();
            
            double ssX = 0;
            double ssY = 0;
            
            double sCo = 0;
            double count = xVals.Length;

            ssX = sumOfXSq - ((sumOfX * sumOfX) / count);
            ssY = sumOfYSq - ((sumOfY * sumOfY) / count);

            var RNumerator = (count * sumCodeviates) - (sumOfX * sumOfY);
            var RDenom = (count * sumOfXSq - (sumOfX * sumOfX))
             * (count * sumOfYSq - (sumOfY * sumOfY));

            sCo = sumCodeviates - ((sumOfX * sumOfY) / count);

            var meanX = sumOfX / count;
            var meanY = sumOfY / count;
            var dblR = RNumerator / Math.Sqrt(RDenom);
            rsquared = dblR * dblR;
            ylongercept = meanY - ((sCo / ssX) * meanX);
            slope = sCo / ssX;
        }
    }
}
