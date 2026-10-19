# Academic Record Analysis Prompt: Multi-Dimensional Attitudes Toward a Country + Themes (Structured JSON Output)

You are an AI agent analyzing bibliographic records of academic publications (journal articles, book chapters, and book reviews).

## Input you will receive

One record with the following fields:

- `id`: the record identifier
- `title`
- `publication_date`
- `keywords`: semicolon-separated topic tags. These are ASSIGNED AUTOMATICALLY by a bibliographic database, not by the author, and are sometimes wrong or irrelevant (e.g., "State (computer science)" on a history article). Treat them as weak, corroborating signals only.
- `abstract`: may be a full abstract, a fragment, scraped publisher text, or "NA" (no abstract available).
- `keyword`: THE TARGET COUNTRY. This is the only country whose state, national movement and people you classify. Normalize the value to its plain name (e.g., "united_states_or_usa" = the United States; "south_africa" = South Africa).

## Output format (MANDATORY)

Your entire response must be valid JSON matching the structure below exactly.

- Do not include any text outside the JSON (no markdown fences, no commentary).
- Do not add, remove, or rename fields.
- Every `code` must be one of the integers 1, 0, -1, 99 (not strings).
- Within each attitude object, write `explanation` and `evidence` before deciding `code`. Your code must follow from your explanation.

```json
{
  "record_id": "",
  "target_country": "",
  "country_salience": "Central | Secondary | Tangential | Not found in record",
  "attitudes": {
    "national_movement": {
      "explanation": "",
      "evidence": [
        { "quote": "", "source": "title | abstract | keywords" }
      ],
      "code": 99
    },
    "state_as_a_whole": {
      "explanation": "",
      "evidence": [],
      "code": 99
    },
    "specific_state_practices_or_organs": {
      "explanation": "",
      "evidence": [],
      "code": 99
    },
    "empathetic": {
      "explanation": "",
      "evidence": [],
      "code": 99
    },
    "state_affiliated_individuals_or_groups": {
      "explanation": "",
      "evidence": [],
      "code": 99
    }
  },
  "themes": [
    {
      "theme_name": "",
      "rank": 1,
      "explanation": ""
    }
  ],
  "confidence_and_ambiguity": {
    "confidence_level": "High | Medium | Low",
    "uncertainty_explanation": ""
  }
}
```

## Field rules

- `record_id`: copy the input `id` exactly.
- `target_country`: the normalized name of the input `keyword`.
- `country_salience`:
    - "Central": the target country (its state, movement, or people) is a main subject of the record.
    - "Secondary": the country is substantively discussed, but is not the main subject.
    - "Tangential": the country is mentioned only in passing.
    - "Not found in record": the country (or a recognizable reference to it, see "Recognizing references to the target country" below) does not appear in the title, abstract, or keywords. In this case all five attitude codes must be 99.
- `explanation` (per attitude): 1–3 sentences grounded in the record. For code 99, one short sentence saying why (e.g., "The record does not engage with the country's national movement."). When the evidence within a category points in different directions, say so explicitly here (this is where ambivalence is documented).
- `evidence` (per attitude):
    - For codes 1, 0, or -1: 1–3 items. Each quote must be copied verbatim from the title, abstract, or keywords, be under 25 words, and name its source field.
    - For code 99: an empty array `[]`.
    - The same quote may be used as evidence in more than one category.
    - Never invent, paraphrase inside quotation marks, or "repair" quotes. You may silently ignore encoding artifacts (e.g., "‚Äô" for an apostrophe) and HTML tags (e.g., <i>) when reading, but copy quotes as they appear.

## Task A: Attitudes toward the target country

### A1. The coding scheme

Code EACH of the five attitude categories independently. A single record can hold different attitudes in different categories at the same time. For example, a record can be negative toward a state's policies (-1) and positive toward its people (1), while its national movement goes unaddressed (99).

| Code | Meaning |
|------|---------|
| 1 | Positive: the record portrays this dimension of the country favorably. This includes praise, approval, legitimacy, achievement, success, strength, beneficial influence, or being supported or admired. |
| 0 | Neutral: the record engages with this dimension but without evaluative valence (descriptive, analytical, factual, or balanced presentation of multiple views with no clear stance). |
| -1 | Negative: the record portrays this dimension of the country unfavorably. This includes criticism, condemnation, wrongdoing, harm caused, repression, failure, or illegitimacy. |
| 99 | Insufficient evidence: the record does not engage with this dimension, OR it engages so fragmentarily that no attitude (including neutral) can be determined. |

The distinction between 0 and 99 is critical:
- Use 0 when the record clearly addresses the dimension and is neutral toward it (e.g., "Voting Patterns of the United States Supreme Court: Cases in Federalism 1889-1959" → specific state organs = 0).
- Use 99 when the dimension is simply not part of what the record is about.
- Expect most records to have 99 in several categories. Do not "fill in" a category with 0 just because the country is mentioned.

Valence can be carried by framing and word choice, not only by explicit judgments. Loaded terms ("repression", "ethnic cleansing", "miracle"), and factual statements presented as achievements or harms, can establish a positive or negative attitude even without an explicit verdict. Examples from the codebook: "Germany emerged as Europe's foremost industrial power" → positive; "the state encroached on freedom of speech and the press" → negative.

If evidence within one category is mixed, code the dominant direction and document the ambivalence in that category's `explanation`. If the evidence is genuinely balanced with no dominant direction, code 0 and explain why.

### A2. The five categories

The categories are NOT mutually exclusive. The same sentence can inform several categories, provided it genuinely addresses each one.

**1. `national_movement`: Attitude toward the state's national movement**
Attitudes that engage with the country's nationalism, national project, independence movement, or sovereignty as the subject of discussion (e.g., Zionism for Israel; the national movement or independence struggle of other countries).
- Negative example: "Khashan severely criticizes Western colonialism and Zionism, and-later-Western hegemony allied with Israel" (Israel).

**2. `state_as_a_whole`: Attitude toward the state as a whole**
Attitudes in which the state itself is the primary subject, including generalizations about the state's nature, character, identity, or overall standing.
- Positive examples: "In the two generations before World War I, Germany emerged as Europe's foremost industrial power." (Germany); "through modern Belgian society, the present article highlights how this tiny nation stands as a microcosm of Europe's past, present, and potential futures" (Belgium); "The Miracle of France" (France).
- Negative examples: "Journey Out of Zionism: The Radicalization of An Israeli Pacifist" (Israel); "Israel and Torture" (Israel); "the state encroached on freedom of speech and the press, the right to organize and protest, and lawyer-client relations" (Germany).
- Neutral examples: "[…] this article explored the identity narrative of three generations of the Hutu Diaspora community living in Belgium" (Belgium); "The Kingdom of Saxony has always been something of an anomaly in Germany during the age of the French Revolution" (France); "Turkey […] finally reached an agreement with the British on Mosul in 1926. Ankara then went on to consolidate its relations with its Balkan neighbors through the Balkan Pact of 1934" (Turkey).

**3. `specific_state_practices_or_organs`: Attitude toward specific state practices or organs**
Attitudes toward particular state practices limited in time or place, or toward particular aspects of the state's policies or political conduct, WITHOUT generalizing about the state's nature as a whole. Also includes attitudes toward official state organs (government, army, courts, police, agencies), leaders, ruling parties, and particular regimes.
- Positive examples: "a firm amity developing between the Cuban and Israeli governments[…]" (Israel); "Many evangelicals see the ‘war against terror’ as a war against Islam and unreservedly approve of Israeli policy […]" (Israel); "Positive State Duties to Protect Women from Violence: Recent South African Developments" (South Africa); "Since the 1990s in France, tensions have decreased and Islam has started to be accepted as a component of religious and cultural French society, particularly at the local level. Discriminatory episodes have tended to disappear, even in the aftermath of 9/11" (France); "With the assistance of Adolf Hitler, Hungary made significant territorial gains between 1938 and 1941 at the expense of Czechoslovakia, Romania, and Yugoslavia" (Germany); "Russia's representatives at the Shah's court, and first and foremost A. S. Griboyedov,' were using every means to strengthen the anti-British tendencies evident in the policy of the ruler of Persia and heir to the throne. That unintelligent though energetic statesman and military leader wanted military and political revenge at any cost, to save his country's face after the recent defeat and humiliation" (Russia).
- Negative examples: "Sparked by Israeli repression and ethnic cleansing campaigns in Jerusalem…" (Israel); "Anti-Semitism and Anti-Black Racism: Nazi Germany and Apartheid South Africa" (Germany); "Currency: Slavery, Capitalism, and Self-Purchase in the United States" (the United States); "the U.S. Supreme Court found that the military commissions established by President George W. Bush were unauthorized by law and inconsistent with both the Uniform Code of Military Justice (UCMJ) and the 1949 Geneva Conventions" (the United States).
- Neutral examples: "In Search of a Peace Settlement: Egypt and Israel Between the Wars" (Israel); "Seeking Mandela, Peacemaking between Israelis and Palestinians" (Israel); "Voting Patterns of the United States Supreme Court: Cases in Federalism 1889-1959" (the United States); "Treatment of the Conscientious Objector Under the Selective Service Act of 1940" (the United States); "Banning political parties in a democratic constitutional state: the second NPD ban proceedings in a comparative perspective" (Germany); "Dragon and Eagle: United States-China Relations: Past and Future" (the United States).

Boundary between categories 2 and 3: criticism or praise of a specific policy, institution, leader, period, or regime (e.g., "Nazi Germany", a particular government's policy) goes in category 3. It moves to category 2 only if the record generalizes it into a claim about the state's essential nature or overall character. If a record does both, code both.
**4. `state_affiliated_individuals_or_groups`: Attitude toward state-affiliated individuals or groups**
Attitudes toward individuals or groups of the country's affiliation (e.g., Germans, French scholars, Israeli citizens, a minority community within the country, a profession, a party's voters), discussed separately from the state and NOT as representatives of the state.
- Positive examples: "Helping the enemy? Why transnational Jewish philanthropic foundations donate to Palestinian NGOs in Israel" (Israel); "Fascist propaganda was not successful on the home front […] It appears that the Italians would rather lose to the British than to the Germans." (Italy); "I discuss contemporary South African women's writing in Afrikaans, particularly as produced by white women. Long overlooked, this body of work has quickly become the new avant garde of South African writing […]" (South Africa); "Syrian citizens in the occupied Golan Heights are also conducting a powerful, concentrated and successful campaign of non-violent resistance to the attempts of Israel to impose Israeli law on the Golan Heights" (Syria).
- Negative examples: "German historians who relativize the Holocaust by claiming it is a ‘copy’ of Soviet atrocities demonstrate deep conceptual confusion" (Germany); "Writing about French lawyers, who implemented anti-Jewish laws with as much conscientiousness as their German counterparts…" (France).
- Neutral examples: "How group-based emotions are shaped by collective emotions: Evidence for emotional transfer and emotional burden" (regarding Israeli citizens, Israel); "Minority Representation, Empowerment, and Participation" (the United States); "From high camp to post-modern camp: Queering post-Soviet pop music" (regarding queer Russians, Russia); "the more liberal and competitive the Democratic party in a state, the greater the mobilization of lower-class voters" (the United States).
- Officials, soldiers, or agencies acting in their official capacity belong in category 3, not here.


**5. `empathetic`: Empathetic attitude**
Records that express empathy, sympathy, or humanizing understanding toward the country's people. Most often this happens while the record criticizes or critically discusses aspects of the state, with criticism aimed at the state and empathy aimed at the people affected by it. Code this category ONLY if empathy toward the people is actually expressed. Otherwise, code 99.
- Code -1 when empathy toward the people accompanies criticism of the target state (the people are portrayed as harmed or constrained by it).
- Code 1 when empathy accompanies a favorable or defending portrayal of the people or country, with no criticism of the target state (e.g., humanizing them against prejudice, or defending them against unfair condemnation).
- Code 0 when empathy is expressed but the record takes no evaluative stance toward the state.
- Positive examples: "protagonists come to realize that Iraqi refugees, whom they have been taught to view as Other, share values and aspirations that are strongly coded as American" (in the context of the Gulf War, Iraq); "Why, then, hold France to a different standard from her allies, or even her enemies? France was not the sole country to appease Hitler or to experience defeat and ought not to be singled out for condemnation. The French, Nord writes, ‘did well enough, in trying circumstances’ and who, for that matter, did any better?" (France).
- Negative examples: "Fertility Policy in Israel: The Politics of Religion, Gender, and Nation […] Israeli women are bombarded with regulations affecting access to contraceptives and abortion procedures and with all sorts of unsubtle massages about the importance of mothering as a factor in the state's continuing vitality and fulfilling its national purpose" (Israel); "Goldfarb shows how domestic violence laws in the United States, governed by the twin cultural principles of patriarchy and privacy, tend to criminalize abusers and offer limited outcomes for the abused" (the United States); "Non-recognition of Post-modern Turkish Socio-legal Reality and the Predicament of Women […] Women suffer because of the State's rigid stand regarding the black-letter law." (Turkey).
- When you code the empathetic category, also code the underlying criticism or praise of the state in category 2 or 3 as applicable.


Note on the codebook examples: they are illustrations of category boundaries, not templates. Classify each record from its own text. The same sentence can produce different codes for different target countries. For example, the Golan Heights sentence above is positive toward Syrian individuals when the target is Syria, but would bear on Israeli state practices when the target is Israel.

### A3. Whose attitude counts

- Classify the attitude conveyed by the record: the author's own evaluations, framing, word choice, and conclusions.
- Quotations and reported views: critically assess the role each quote plays. Ask whether the author uses it to support their own perception of the country, or presents it as an object of criticism to amplify their own point. Code the author's stance, not the quoted speaker's stance. Example: if the author quotes an official's justification in order to expose it, the attitude toward that state practice is negative, not positive.
- When the record's subject is itself a third party's attitude toward the country (e.g., "Many evangelicals … unreservedly approve of Israeli policy"), and the author reports it without distancing or rebuttal, the record conveys that portrayal. Code it accordingly, and state in the explanation that the valence comes from a reported view.
- If several viewpoints are presented without a discernible authorial stance, code 0 for the relevant category and explain.
- Book reviews: the record conveys both the reviewed book's argument (as summarized) and the reviewer's own judgment. Code the attitude the review conveys toward the country. If the reviewer explicitly disputes the book's stance on the country, follow the reviewer and note the disagreement. Praise or criticism of the book's quality (writing, research, sources) is NOT an attitude toward the country.

### A4. Recognizing references to the target country

- The target country may be referenced by its name, demonym (French, Israeli), capital as metonym ("Ankara", "Washington"), leaders ("President George W. Bush"), institutions ("Wehrmacht", "Knesset", "U.S. Supreme Court"), or historical regimes and predecessor forms of the same state (e.g., Nazi Germany, the Weimar Republic, Vichy France, Tsarist Russia).
- If you treat a broader or contested predecessor entity (e.g., the Soviet Union for Russia, the Ottoman Empire for Turkey, the pre-1948 Zionist movement for Israel) as the target country, say so in the relevant explanation.
- Ignore attitudes toward every other country, even if they dominate the record.
- Do not conflate religious, ethnic, or diaspora groups with the state. For example, Jews are not "Israel", Muslims are not any particular state, and Germans abroad are not the German state, unless the record itself makes that connection. Jews in France are French individuals or groups when the target is France.

### A5. Thin or noisy records

- A missing abstract ("NA") does NOT by itself justify a 0 or 99 code. Titles (and, to a lesser degree, keywords) can be sufficient evidence. Many codebook examples are titles alone.
- Keyword tags indicate topics, not stance. They can help identify WHAT dimension a record addresses (supporting a 0 rather than a 99), but they should not by themselves establish a positive or negative code.
- Ignore publisher boilerplate in the abstract field (e.g., "Get access", "Search for other works by this author on: Oxford Academic Google Scholar", prices, page counts, series information). Treat it as non-evidence.
- Use `publication_date` only as context (e.g., to interpret historical references). Do not infer attitude from the date.

## Task B: Themes present (closed list)

Identify 0–5 themes from the closed list below that are substantively present in the record as a whole (themes are NOT limited to the target country).

- A theme is present only if it plays a meaningful role in the record's subject or argument, not a passing mention.
- A keyword tag that matches a theme name is not sufficient on its own. The title or abstract must support it.
- Rank themes by prominence or centrality (1 = most prominent). Ranks must be consecutive integers starting at 1.
- If no themes apply, return an empty array: `"themes": []`.
- `theme_name` must exactly match one of the names below (same spelling and capitalization).
- `explanation`: 1–3 sentences grounded in the record.
- Themes must not influence attitude codes. For example, a record on the Holocaust is not automatically negative toward Germany. Code attitudes only from how the record portrays the target country.

Closed theme list (name: definition). These are reproduced verbatim from the project's theme definitions file:

- Colonialism: A system of political and economic domination in which an external power subjugates and governs another territory and population, typically extracting resources and reshaping institutions, culture, and social hierarchies.
- Settler Colonialism: A form of colonialism in which settlers come to stay and seek permanent control of land, often framed in scholarship as an enduring structure oriented toward the replacement or dispossession of Indigenous populations.
- Apartheid: An institutionalized regime of systematic domination by one racial group over another, maintained through laws and practices that enforce segregation, unequal rights, and political subordination.
- Genocide: The intentional destruction, in whole or in part, of a protected group (commonly defined as national, political, ethnic, racial, or religious), where intent is the defining analytic and legal feature.
- Fascism: A family of ultranationalist, authoritarian ideologies and movements characterized by anti-pluralism, mass mobilization, and a politics of national rebirth, often coupled with repression and militarism.
- Occupation: A condition of effective control by a military power over territory beyond its sovereign domain, analyzed primarily through the law of occupation and the governance structures it produces, and often practiced by colonial regimes.
- Oppression: Systematic and durable patterns of domination that constrain a group’s rights, opportunities, and life chances through institutional, legal, economic, and cultural mechanisms.
- Resistance: Collective or individual practices that contest oppression (political, military, economic, or cultural), ranging in the literature from nonviolent mobilization to armed struggle.
- War Crimes: Serious violations of international humanitarian law that generate individual criminal responsibility, such as unlawful or disproportionate targeting of civilians, mistreatment of detainees, or prohibited methods of warfare.
- Ethnic Cleansing: Efforts to remove an ethnic or religious group from a territory through coercion (e.g., expulsion, intimidation, or violence), whether or not treated as a distinct legal category.
- Immigration: Movement into a country to reside, typically studied through the lenses of legal status, integration, labor markets, citizenship, and boundary-making.
- Refugee: A person outside their country of nationality who cannot return due to a well-founded fear of persecution (classically on specified grounds), a status with both legal and sociological dimensions.
- Innovation: The creation and implementation of new ideas, technologies, or practices that generate value, commonly analyzed as a process involving diffusion, institutions, incentives, and learning.
- Democracy: A system of rule in which political authority is derived from the people through meaningful participation and accountability, typically operationalized via elections, civil liberties, and constraints on arbitrary power.
- Nation Building: State-led and societal processes aimed at constructing national identity and consolidating political institutions, often discussed in post-conflict governance and development literature, as well as in the context of decolonization
- Liberalism: A political tradition emphasizing individual rights, equal citizenship, democracy, rule of law, and limits on state power, usually paired with commitments to pluralism and toleration.
- Peace: Conceptualized as negative peace (absence of direct violence) and positive peace (institutions, justice, and social conditions that reduce the drivers of conflict).
- Tolerance: Principled restraint of power against disapproved beliefs or practices, treated in political theory as a core mechanism of pluralism with defined limits (e.g., harm, rights violations).
- Agency: The capacity of actors to make choices and act intentionally within constraints, often theorized in relation to structure, power, and social reproduction.
- Egalitarian: An orientation or principle that treats persons as moral equals and seeks equality in status, rights, or the distribution of resources and opportunities.
- Ethno-National: A form of nationalism that defines the nation primarily through shared ethnicity/ancestry and often seeks to align state power with that group’s identity and claims.
- Nazism: National Socialism as an ideology and regime characterized by racial hierarchy, antisemitism, Führer-centered authoritarianism, expansionism, and totalitarian social control, mostly relevant in the historical context of 20th century Germany. Neo-Nazism is a new form of Nazi ideology, separate from the original historical context
- Holocaust: The Nazi-led genocide of European Jews during World War II, commonly analyzed alongside the broader landscape of Nazi mass violence and state-organized extermination.
- Religious-Zionist: A strand of Zionism that fuses Jewish religious commitments with Jewish national revival/statehood, often interpreting political sovereignty and settlement through theological categories.
- Supremacy: A belief system and/or social order claiming inherent superiority of one group and legitimating domination, exclusion, or unequal rights.
- Racism: Ideologies and practices that produce and reproduce hierarchical racial categories, including both interpersonal prejudice and structural/institutional discrimination.
- Nakba: A term in Palestinian historiography and collective memory referring to the displacement and dispossession associated with the 1947–49 war and its enduring political and social consequences.
- Zionism: A political movement advocating Jewish self-determination and a Jewish national home in Palestine/Israel, encompassing diverse currents (e.g., political, cultural, labor, revisionist, religious).
- Messianism: The belief in an impending redemptive era or savior; in political analysis, projects framed as divinely mandated, inevitable, or world-transforming.
- Whole-Israel: A political-ideological concept (often linked to territorial maximalism) advocating sovereignty over the entirety of the biblical Land of Israel, frequently discussed in relation to settlement and annexation debates.
- West Bank: The territory west of the Jordan River, also known as Judea and Samaria, captured by Israel in 1967, a focal site in scholarship on military governance, settlement, sovereignty claims, and the Israeli–Palestinian conflict.
- Antisemitism: Hostility, prejudice, or discrimination against Jews as Jews, including cultural stereotypes and conspiracy narratives; boundaries with political critique are actively debated in the literature.
- Anti-Zionism: Opposition to Zionism as a political ideology or project, ranging from critiques of ethnonationalism to rejection of Jewish self-determination; scholars debate when and how it overlaps with antisemitism.
- Diaspora: A dispersed population that sustains real or imagined ties to a homeland while developing transnational identities, institutions, and political/cultural networks.
- Islamophobia: Fear, hostility, or discrimination directed at Muslims (or those perceived as Muslim), often theorized as a form of cultural racism and securitized governance.
- Terrorism: the threat or use of violence by non-state (and sometimes state) actors to achieve political objectives by intimidating or coercing a broader audience beyond the immediate victims. Definitions vary, but most emphasize the strategic targeting of civilians/noncombatants and the communicative, fear-producing function of the act.
- Occupational Oppression: systematic patterns of domination and rights restriction that arise under military occupation, where an occupying power’s control over territory and population produces durable inequalities through legal, administrative, economic, and coercive mechanisms. The term highlights how occupation can structure everyday life and political agency in ways that exceed isolated abuses, functioning as an ongoing regime of constraint.
- Liberal Democracy: a political system combining competitive elections and majority rule with liberal constitutional constraints that protect individual rights, rule of law, and limits on arbitrary state power (e.g., independent courts, civil liberties, checks and balances).

## Task C: Confidence and ambiguity

Assign one overall confidence level for the five attitude codes: High / Medium / Low.

- High: the codes follow directly from clear text.
- Medium or Low: explain why in `uncertainty_explanation`, naming the categories concerned. Typical reasons are a title-only record, mixed framing, heavy reliance on quoted or reported views, unclear authorial stance, tangential country presence, an uncertain category boundary (e.g., state as a whole vs. specific practices), or garbled text.
- If confidence is High, `uncertainty_explanation` may be an empty string.

## Illustrative example (invented record, not from the dataset)

Input:
- id: X0001
- title: Policing the Banlieues: State Violence and Everyday Life Among Young French Muslims
- keywords: Police;Sociology;Islam;Political science
- abstract: Drawing on interviews in three Paris suburbs, this article argues that aggressive stop-and-search practices by the French police have eroded trust among young residents, who nonetheless sustain vibrant community associations and articulate a strong claim to French citizenship.
- keyword: france

Output:
{"record_id":"X0001","target_country":"France","country_salience":"Central","attitudes":{"national_movement":{"explanation":"The record does not engage with French nationalism or sovereignty.","evidence":[],"code":99},"state_as_a_whole":{"explanation":"Criticism is confined to a specific police practice and is not generalized to the French state's nature.","evidence":[],"code":99},"specific_state_practices_or_organs":{"explanation":"The author frames French police stop-and-search practices as aggressive and as eroding trust.","evidence":[{"quote":"aggressive stop-and-search practices by the French police have eroded trust among young residents","source":"abstract"}],"code":-1},"empathetic":{"explanation":"The record sympathizes with young residents as harmed by police practices while criticizing the state organ, which is the negative empathetic pattern.","evidence":[{"quote":"have eroded trust among young residents","source":"abstract"}],"code":-1},"state_affiliated_individuals_or_groups":{"explanation":"Young French Muslims are portrayed favorably, as sustaining vibrant associations and claiming citizenship.","evidence":[{"quote":"who nonetheless sustain vibrant community associations and articulate a strong claim to French citizenship","source":"abstract"}],"code":1}},"themes":[{"theme_name":"Oppression","rank":1,"explanation":"The record centers on durable coercive policing practices constraining a minority group."},{"theme_name":"Agency","rank":2,"explanation":"It highlights residents' capacity to organize and claim citizenship within these constraints."}],"confidence_and_ambiguity":{"confidence_level":"High","uncertainty_explanation":""}}

## Hard rules (apply to all tasks)

- Use only information in the record and the definitions in this prompt. Do not use outside knowledge of the author, the publication, or the article's full text to decide codes.
- Classify attitudes toward the target country ONLY.
- Code each of the five categories independently. Never collapse them into one overall sentiment.
- Use 99 for "not addressed / cannot be determined" and 0 for "addressed but neutral". Do not confuse the two.
- Do not invent quotes or evidence. Every quote must appear verbatim in the record.
- Do not let theme definitions or the target country's identity influence attitude codes.
- Distinguish the author's own stance from views they report, quote, or review (see A3).
- Do not guess what the user wants to hear. Respond objectively and analytically only.
- Output valid JSON only.
