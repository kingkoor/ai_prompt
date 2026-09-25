Hi Glen,

I checked GoCanvas Salesforce. It doesn't have the state/country code fields that Nexus has; the picklists are turned off there, so there's nothing extra to extract. The custom fields I found (State__c, the Marketo "inferred" fields) are either full names or guesses, and often a different state from the mailing address.

In GoCanvas, users type the full state name, like "Texas" or "Ontario". This causes two problems:

The state comes out blank. Our function only keeps the state when the country is a code like "US". GoCanvas sends "United States", so every GoCanvas state is dropped.
The same address gets two AddressKeys.
Brand	State	Country	AddressKey
Nexus	TX	US	key A
GoCanvas	Texas	United States	key B

Same office, two rows in dim_addresses.

My suggestion: a small change to normalize_state_province:

change the country to a code first ("United States" → "US");
change full state names to codes ("Texas" → "TX", "Ontario" → "ON", "New South Wales" → "NSW"), for US, Canada and Australia only.

Values that are already codes pass through unchanged, so Nexus, Chargebee and NetSuite keys don't change. Only GoCanvas gets fixed.

OK to go ahead?
