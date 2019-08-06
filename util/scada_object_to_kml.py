from scada_gateway import ScadaGateway

scada = ScadaGateway('http://10.5.165.42:8080', 'admin', 'passme')
scada.logon()

obj = scada.model_objstruct_object_by_type('OTYP_RECLOSER')
scada.logoff()

placemark = """
        <Placemark>
            <name>{}</name>
            <guid>{}</guid>
			<styleUrl>#pointStyleMap</styleUrl>
			<ExtendedData>
				<SchemaData schemaUrl="#S_exit_SDD">
					<SimpleData name="Реклоузер">{}</SimpleData>
				</SchemaData>
			</ExtendedData>
			<Point>
				<coordinates>{},{},0</coordinates>
			</Point>
		</Placemark>
"""

for item in obj:
    guid = item.get('Uid')
    name = item.get('Name')
    geolocation = item.get('GeoLocation')

    if guid and name and geolocation:
        print(placemark.format(name, guid, name, geolocation['Longitude'], geolocation['Latitude']))
