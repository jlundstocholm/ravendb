﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using Lucene.Net.Documents;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Indexing
{
	[CLSCompliant(false)]
	public static class AnonymousObjectToLuceneDocumentConverter
	{
		public static IEnumerable<AbstractField> Index(object val, PropertyDescriptorCollection properties, IndexDefinition indexDefinition, Field.Store defaultStorage)
		{
			return (from property in properties.Cast<PropertyDescriptor>()
			        let name = property.Name
			        where name != "__document_id"
			        let value = property.GetValue(val)
			        from field in CreateFields(name, value, indexDefinition, defaultStorage)
			        select field);
		}

        public static IEnumerable<AbstractField> Index(JObject document, IndexDefinition indexDefinition, Field.Store defaultStorage)
        {
        	return (from property in document.Cast<JProperty>()
        	        let name = property.Name
        	        where name != "__document_id"
        	        let value = GetPropertyValue(property)
        	        from field in CreateFields(name, value, indexDefinition, defaultStorage)
        	        select field);
        }

        private static object GetPropertyValue(JProperty property)
        {
            switch (property.Value.Type)
            {
                case JTokenType.Array:
                case JTokenType.Object:
                    return property.Value.ToString(Formatting.None);
                default:
                    return property.Value.Value<object>();
            }
        }

		/// <summary>
		/// This method generate the fields for indexing documents in lucene from the values.
		/// Given a name and a value, it has the following behavior:
		/// * If the value is null, create a single field with the supplied name with the unanalyzed value 'NULL_VALUE'
		/// * If the value is string or was set to not analyzed, create a single field with the supplied name
		/// * If the value is date, create a single field with millisecond precision with the supplied name
		/// * If the value is numeric (int, long, double, decimal, or float) will create two fields:
		///		1. with the supplied name, containing the numeric value as an unanalyzed string - useful for direct queries
		///		2. with the name: name +'_Range', containing the numeric value in a form that allows range queries
		/// </summary>
		private static IEnumerable<AbstractField> CreateFields(string name, object value, IndexDefinition indexDefinition, Field.Store defaultStorage)
		{
			if (value == null)
			{
				yield return new Field(name, "NULL_VALUE", indexDefinition.GetStorage(name, defaultStorage),
								 Field.Index.NOT_ANALYZED);
				yield break;
			}

			if (indexDefinition.GetIndex(name, Field.Index.ANALYZED) == Field.Index.NOT_ANALYZED || value is string)
			{
				yield return new Field(name, value.ToString(), indexDefinition.GetStorage(name, defaultStorage),
								 indexDefinition.GetIndex(name, Field.Index.ANALYZED));
				yield break;
			}

			if (value is DateTime)
			{
				yield return new Field(name, DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND),
					indexDefinition.GetStorage(name, defaultStorage),
					indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED));
			}
			else if(value is IConvertible) // we need this to store numbers in invariant format, so JSON could read them
			{
				var convert = ((IConvertible) value);
				yield return new Field(name, convert.ToString(CultureInfo.InvariantCulture), indexDefinition.GetStorage(name, defaultStorage),
				                       indexDefinition.GetIndex(name, GetDefaultIndexOption(value)));
			}
			else 
			{
				yield return new Field(name + "_ConvertToJson", "true", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
				yield return new Field(name, JToken.FromObject(value).ToString(), indexDefinition.GetStorage(name, defaultStorage),
				                       indexDefinition.GetIndex(name, GetDefaultIndexOption(value)));
			}


			if (value is int)
			{
				yield return new NumericField(name + "_Range", indexDefinition.GetStorage(name, defaultStorage), true)
					.SetIntValue((int)value);

			}
			if (value is long)
			{
				yield return new NumericField(name + "_Range", indexDefinition.GetStorage(name, defaultStorage), true)
					.SetLongValue((long) value);

			}
			if (value is decimal)
            {
				yield return new NumericField(name + "_Range", indexDefinition.GetStorage(name, defaultStorage), true)
					.SetDoubleValue((double)(decimal)value);
            }
			if (value is float)
            {
            	yield return new NumericField(name + "_Range", indexDefinition.GetStorage(name, defaultStorage), true)
            		.SetFloatValue((float) value);
            }
			if (value is double)
            {
				yield return new NumericField(name + "_Range", indexDefinition.GetStorage(name, defaultStorage), true)
					.SetDoubleValue((double)value);
            }
		}

    	private static Field.Index GetDefaultIndexOption(object value)
    	{
			if(value is long || value is int || value is decimal || value is float || value is double)
				return Field.Index.NOT_ANALYZED;
    		return Field.Index.ANALYZED;
    	}
	}
}