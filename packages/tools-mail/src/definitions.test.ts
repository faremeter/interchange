// The schema a mail tool advertises in definitions.ts is the only description
// of its arguments a model ever sees, and every argument shape in handlers.ts
// carries arktype's `"+": "reject"`. A parameter the schema omits is therefore
// unreachable, and a key the model guesses costs the call instead of being
// ignored, so the two descriptions have to agree key for key.
//
// Both sides are derived rather than listed here: the advertised side by
// parsing TOOL_DEFINITIONS, the accepted side by introspecting the arktype
// shapes and, for the value an omitted argument resolves to, by reading the
// fallback the handler applies. A shape that gains a key fails these tests
// until the schema catches up.
//
// arktype 2.2 key introspection, all public members of the Type surface:
//
//   - `props` yields one entry per declared key, carrying `key`, `kind`
//     ("required" or "optional") and `value` (the key's own Type).
//   - `extract` and `exclude` select the branches of a union, and `equals`
//     compares against a definition, which is how a branch is tested for
//     emptiness.
//   - `in` is the input side of a morph, and `select(kind)` yields the nodes of
//     one kind a shape compiled to: `"divisor"`, `"min"`, `"max"` and
//     `"pattern"` the constraints a bounded or matched shape carries.
//     `distribute` yields a union's own branches. `select("unit")` also finds
//     literals nested inside an object or an array element, which this
//     comparison does not cover.
//
// `toJsonSchema` looks like the shorter route and is not usable here: it
// renders the recursive query shape with a `$ref` to a `$defs` entry it never
// emits, and throws on the date filters' morph before reaching it.
//
// What the comparison below covers is therefore each key, whether it is
// required, which JSON types it accepts, the constraints it puts on the value,
// the values of an enumerated key, and the default the key advertises.
// The element type of an array key is not compared: the query's 'and' and 'or'
// filters are arrays of the query shape itself, and arktype answers no question
// about that element -- `extends("object[]")` is false, `extract("object[]")`
// is never, and `allows([{}])` raises a TypeError from inside the compiled
// validator -- so an element check would hold for some array keys and not
// others.

import { describe, expect, test } from "bun:test";
import { scope, type } from "arktype";

import { TOOL_DEFINITIONS, type MailToolName } from "./definitions";
import {
  ARGUMENT_SHAPES,
  SearchQueryArgs,
  makeMailExpungeHandler,
  makeMailFlagHandler,
  makeMailReadHandler,
  makeMailReplyHandler,
  makeMailSearchHandler,
  makeMailSendHandler,
  makeMailWaitHandler,
} from "./handlers";

// ---------------------------------------------------------------------------
// Value constraints, in the one vocabulary both sides are rendered into
// ---------------------------------------------------------------------------

// What a key says about its value beyond naming a JSON type. Each side derives
// this from its own description and renders it through renderConstraints, so a
// disagreement reads as a diff of two constraint expressions rather than of two
// vocabularies.
type Bound = { exclusive: boolean; rule: number };

type Constraints = {
  divisor: number | undefined;
  min: Bound | undefined;
  max: Bound | undefined;
  pattern: string | undefined;
};

function renderConstraints(constraints: Constraints): string {
  const parts: string[] = [];
  if (constraints.divisor !== undefined) {
    parts.push(
      constraints.divisor === 1
        ? "integer"
        : `multiple of ${String(constraints.divisor)}`,
    );
  }
  if (constraints.min !== undefined) {
    const operator = constraints.min.exclusive ? ">" : ">=";
    parts.push(`${operator} ${String(constraints.min.rule)}`);
  }
  if (constraints.max !== undefined) {
    const operator = constraints.max.exclusive ? "<" : "<=";
    parts.push(`${operator} ${String(constraints.max.rule)}`);
  }
  if (constraints.pattern !== undefined) {
    parts.push(`matching ${constraints.pattern}`);
  }
  return parts.join(" & ");
}

// ---------------------------------------------------------------------------
// The advertised side: the JSON Schema subset definitions.ts uses
// ---------------------------------------------------------------------------

// Both shapes below reject a keyword they do not declare. A keyword nothing
// compares is how the two descriptions drift while looking guarded: it steers a
// model applying constrained decoding, no handler is held to it, and an open
// shape here would accept it in silence. Rejecting it fails these tests until
// the comparison below learns to derive an enforced counterpart for it.
//
// Two of the declared keywords are carried rather than compared. 'description'
// is prose, and the enforced side has none to disagree with. 'items' names an
// array's element type, which is not compared for the reason the header gives.
// Every other keyword is held to an enforced counterpart: the types and the
// constraints to the arktype shape, and 'default' to the fallback the handler
// applies -- which `props` cannot see, so it is read from the handler's own
// text instead; see ARGUMENT_FALLBACK.

const AdvertisedObject = type({
  "+": "reject",
  type: "'object'",
  properties: "Record<string, unknown>",
  "required?": "string[]",
  "description?": "string",
});

const AdvertisedProperty = scope({
  property: {
    "+": "reject",
    "type?": "'string' | 'number' | 'integer' | 'boolean' | 'object' | 'array'",
    "anyOf?": "property[]",
    "enum?": "string[]",
    "minimum?": "number",
    "maximum?": "number",
    "exclusiveMinimum?": "number",
    "exclusiveMaximum?": "number",
    "multipleOf?": "number",
    "pattern?": "string",
    "items?": "property",
    "properties?": "Record<string, unknown>",
    "required?": "string[]",
    "description?": "string",
    // Declared so the value can be read. The inferred type names the keys this
    // shape declares and no others, so the comparison below can ask a property
    // for its 'default' only because the key is declared here; a schema
    // carrying one is a question of reachability rather than of presence.
    "default?": "unknown",
  },
}).export().property;

type AdvertisedProperty = typeof AdvertisedProperty.infer;

// The JSON types a property advertises, so that a disagreement reads as a diff
// of two type expressions.
function advertisedTypes(
  property: AdvertisedProperty,
  where: string,
): Set<string> {
  const types = new Set<string>();
  for (const branch of property.anyOf ?? []) {
    for (const branchType of advertisedTypes(branch, where)) {
      types.add(branchType);
    }
  }
  if (property.type !== undefined) {
    // JSON Schema's 'integer' names a number whose fractional part is zero, so
    // it is the 'number' type carrying a constraint rather than a type of its
    // own -- and arktype has no integer domain either. The constraint half is
    // compared separately; see advertisedConstraints.
    types.add(property.type === "integer" ? "number" : property.type);
  }
  if (types.size === 0) {
    throw new Error(`${where} advertises neither a 'type' nor an 'anyOf'`);
  }
  return types;
}

// One edge of a numeric range. JSON Schema spells the inclusive and the
// exclusive form as separate keywords, and a property carrying both says two
// things about the same edge, which is a defect in the schema rather than a
// disagreement with the handler.
function advertisedBound(
  inclusive: number | undefined,
  exclusive: number | undefined,
  where: string,
): Bound | undefined {
  if (inclusive !== undefined && exclusive !== undefined) {
    throw new Error(`${where} is advertised as both inclusive and exclusive`);
  }
  if (inclusive !== undefined) return { exclusive: false, rule: inclusive };
  if (exclusive !== undefined) return { exclusive: true, rule: exclusive };
  return undefined;
}

// The constraints a property advertises. 'type: "integer"' and
// 'multipleOf: 1' assert the same thing, and the shape that enforces either
// compiles to a divisor of 1, so both spellings normalise to that divisor.
function advertisedConstraints(
  property: AdvertisedProperty,
  where: string,
): Constraints {
  let divisor = property.multipleOf;
  if (divisor === undefined && property.type === "integer") divisor = 1;
  return {
    divisor,
    min: advertisedBound(
      property.minimum,
      property.exclusiveMinimum,
      `${where}'s lower bound`,
    ),
    max: advertisedBound(
      property.maximum,
      property.exclusiveMaximum,
      `${where}'s upper bound`,
    ),
    pattern: property.pattern,
  };
}

// ---------------------------------------------------------------------------
// The accepted side: the arktype argument shapes
// ---------------------------------------------------------------------------

// The shapes a bare alias reference can name, keyed by the reference arktype
// renders for it; see acceptedTypes below.
const SCOPE_ALIASES = new Map<string, type.Any>([
  ["$searchQuery", SearchQueryArgs],
]);

// One entry of a shape's `props`, widened to what this file reads. `type.Any`
// is arktype's own "some Type" and carries every method used below.
type AcceptedProp = {
  readonly kind: "required" | "optional";
  readonly key: string | symbol;
  readonly value: type.Any;
};

// The JSON types a shape accepts for one key, in the same rendering as
// advertisedTypes.
//
// A tool call arrives as JSON, and two consequences follow. A morph is reached
// through its input, so `in` is the side a call supplies -- the date filters
// declare `string.date.parse`, whose input is the string a caller sends. And
// JSON carries no Date instance, so the Date branch of a date filter names no
// value a call can carry and is excluded before classifying.
function acceptedTypes(value: type.Any, where: string): Set<string> {
  const callable = value.in.exclude("Date");
  const types = new Set<string>();
  if (!callable.extract("string").equals("never")) types.add("string");
  if (!callable.extract("number").equals("never")) types.add("number");
  if (!callable.extract("boolean").equals("never")) types.add("boolean");

  if (!callable.extract("unknown[]").equals("never")) types.add("array");

  // An array is an object, so the object branches are what is left once the
  // array branches are removed.
  if (!callable.exclude("unknown[]").extract("object").equals("never")) {
    types.add("object");
  }

  if (types.size > 0) return types;

  // A key whose shape is a bare reference to an alias of its own scope -- the
  // query's 'not' filter, which is the query shape itself -- is opaque to
  // these predicates: every domain answers false, the object domain included,
  // where the resolved shape answers true. arktype renders such a reference as
  // "$name", and an array of one still answers "array" above, so only the bare
  // reference reaches here. SCOPE_ALIASES names the shape behind it, which is
  // classified in its place, so the type the schema advertises for such a key
  // is still held to something.
  const resolved = SCOPE_ALIASES.get(value.expression);
  if (resolved !== undefined) return acceptedTypes(resolved, where);

  throw new Error(
    `${where} accepts ${value.expression}, which names no JSON type`,
  );
}

// A key carries at most one constraint of each kind, because one keyword is all
// a schema has to advertise it with. More than one means the shape is a union
// whose branches constrain the same value differently, which is a shape no
// property in definitions.ts can describe, so it is reported rather than
// rendered as whichever node came first.
function soleNode<T>(nodes: readonly T[], where: string): T | undefined {
  if (nodes.length > 1) {
    throw new Error(
      `${where}: the shape enforces ${String(nodes.length)} of them, which one keyword cannot advertise`,
    );
  }
  return nodes[0];
}

// The constraints a shape enforces for one key, in the same rendering as
// advertisedConstraints. `select` answers for each kind of constraint node the
// shape compiled to: `number.integer` compiles to a divisor of 1, `atLeast` and
// `atMost` to a min and a max, and `matching` to a pattern.
//
// A `narrow` compiles to an opaque predicate and answers none of these, so a
// constraint written as a narrow is one this comparison cannot see. That is why
// the shapes in handlers.ts state what they can as bounds and patterns: a
// narrow is unavoidable where the rule spans two keys, and a schema cannot
// advertise those either.
//
// Each kind is selected from the branch that can carry it -- the number branch
// for the numeric bounds, the string branch for the pattern -- because
// selecting across a whole union would report one branch's constraint as the
// key's. `in` and the Date exclusion are there for the reasons acceptedTypes
// gives.
function acceptedConstraints(value: type.Any, where: string): Constraints {
  const callable = value.in.exclude("Date");
  const numbers = callable.extract("number");

  const divisor = soleNode(numbers.select("divisor"), `${where}'s divisor`);
  const min = soleNode(numbers.select("min"), `${where}'s lower bound`);
  const max = soleNode(numbers.select("max"), `${where}'s upper bound`);
  const pattern = soleNode(
    callable.extract("string").select("pattern"),
    `${where}'s pattern`,
  );

  return {
    divisor: divisor === undefined ? undefined : divisor.rule,
    min:
      min === undefined
        ? undefined
        : { exclusive: min.exclusive === true, rule: min.rule },
    max:
      max === undefined
        ? undefined
        : { exclusive: max.exclusive === true, rule: max.rule },
    pattern: pattern === undefined ? undefined : pattern.rule,
  };
}

// The literal values an enumerated key accepts, empty for every other key.
// This is what holds the 'type' parameter's enum to the InterchangeType union
// the handlers validate against.
//
// Only a unit that is itself a branch of the key counts. `select("unit")`
// also finds literals nested inside an object or an array element, and those
// describe a field this comparison does not cover: the header says an array's
// element type is carried, not compared. A branch is that unit when the only
// unit it contains is the branch itself.
function acceptedValues(value: type.Any): string[] {
  const values: string[] = [];
  for (const branchValues of value.distribute((branch) => {
    const units = branch.select("unit");
    const only = units[0];
    if (units.length !== 1 || only === undefined) return [];
    if (only.expression !== branch.expression) return [];
    return [String(only.unit)];
  })) {
    values.push(...branchValues);
  }
  return values.sort();
}

// ---------------------------------------------------------------------------
// The enforced side: the value substituted for an omitted argument
// ---------------------------------------------------------------------------

// A default is the one advertised fact a model acts on by leaving the argument
// out: it reads `default: 20`, sends no 'limit', and expects the 20 back. What
// it gets is the fallback the handler applies at the tool-argument boundary --
// `args.limit ?? 20` -- which sits inside the handler closure, where no shape
// describes it and no call reports it without a transport to run the handler
// against.
//
// So it is read from the handler's own text. A factory's `toString` is the body
// bun compiled from handlers.ts, which is the code that runs: the comments are
// gone and the declarations are merged, so what the match sees is the
// statements rather than their formatting.
//
// The alternatives are both worse. A list of expected values written here would
// restate the schema and assert nothing. Driving each handler for the value it
// substitutes needs a MessageTransport stub -- one exists in tools-mail.test.ts
// and another in mail-wait-settlement.test.ts, and importing either runs that
// file's suite -- and mail_wait's default would stay out of reach even with one,
// because the only place a 120-second deadline is observable is at the end of
// it.
const ARGUMENT_FALLBACK =
  /\bargs\.([A-Za-z_$][\w$]*)\s*\?\?\s*("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?|\[\]|\{\}|true|false|null)/g;

// Only the text of a factory is read, so its shape is immaterial here; what
// matters is that every tool names one, which the `satisfies` enforces.
type HandlerFactory = (...args: never[]) => unknown;

const HANDLER_FACTORIES = new Map<string, HandlerFactory>(
  Object.entries({
    mail_send: makeMailSendHandler,
    mail_reply: makeMailReplyHandler,
    mail_search: makeMailSearchHandler,
    mail_read: makeMailReadHandler,
    mail_wait: makeMailWaitHandler,
    mail_flag: makeMailFlagHandler,
    mail_expunge: makeMailExpungeHandler,
  } satisfies Record<MailToolName, HandlerFactory>),
);

type EnforcedDefaults = ReadonlyMap<string, unknown>;

// A nested shape substitutes nothing: a fallback applies to the arguments a tool
// is called with, and 'ref', 'query' and 'query.header' are validated whole,
// with the keys inside them left as the caller sent them.
const NO_ENFORCED_DEFAULTS: EnforcedDefaults = new Map();

// The fallbacks that advertise no default, and should not. A 'default' promises
// a value the caller can use, and an empty container is not one: an omitted
// 'query' becomes the empty query, which the query schema's own description
// already names as matching every message, and an omitted 'set' or 'clear'
// becomes an empty flag list that the flag handler then refuses as a call
// mutating nothing. Advertising either would read as permission to omit the
// argument, so a default appearing on one of these fails the pairing below.
const UNADVERTISED_FALLBACKS: ReadonlySet<string> = new Set([
  "mail_search.query",
  "mail_wait.query",
  "mail_flag.set",
  "mail_flag.clear",
]);

// The value a tool's handler substitutes for each omitted argument. A fallback
// written in any form other than the literals above -- a named constant, a
// destructuring default -- matches nothing and leaves its key out, which fails
// the pairing below as an advertised default the handler does not honour rather
// than passing unnoticed.
function enforcedDefaults(tool: string): EnforcedDefaults {
  const factory = HANDLER_FACTORIES.get(tool);
  if (factory === undefined) {
    throw new Error(`no handler answers for the tool "${tool}"`);
  }

  const defaults = new Map<string, unknown>();
  for (const match of factory.toString().matchAll(ARGUMENT_FALLBACK)) {
    const [, key, literal] = match;
    if (key === undefined || literal === undefined) {
      throw new Error(`${tool}: a fallback matched with no key or no value`);
    }
    const value: unknown = JSON.parse(literal);
    if (
      defaults.has(key) &&
      JSON.stringify(defaults.get(key)) !== JSON.stringify(value)
    ) {
      throw new Error(
        `${tool} substitutes two different values for an omitted '${key}', so it advertises neither`,
      );
    }
    defaults.set(key, value);
  }
  return defaults;
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

// `default` is the value the key resolves to when a call omits it, and
// undefined means the key names none.
type KeyFact = {
  required: boolean;
  types: string;
  constraints: string;
  values: string[];
  default: unknown;
};
type KeyFacts = Record<string, KeyFact>;

function renderTypes(types: Set<string>): string {
  return [...types].sort().join(" | ");
}

// One line per key, so that a disagreement shows as a diff of two lines that
// each name their key.
function renderFacts(facts: KeyFacts): Record<string, string> {
  const rendered: Record<string, string> = {};
  for (const [key, fact] of Object.entries(facts)) {
    const parts = [fact.required ? "required" : "optional", fact.types];
    if (fact.constraints.length > 0) parts.push(fact.constraints);
    if (fact.values.length > 0) parts.push(`one of: ${fact.values.join(", ")}`);
    if (fact.default !== undefined) {
      parts.push(`default: ${JSON.stringify(fact.default)}`);
    }
    rendered[key] = parts.join("; ");
  }
  return rendered;
}

function advertisedFacts(rawSchema: unknown, where: string): KeyFacts {
  const schema = AdvertisedObject(rawSchema);
  if (schema instanceof type.errors) {
    throw new Error(`${where} is not an object schema: ${schema.summary}`);
  }

  const undeclaredRequired = new Set(schema.required ?? []);
  const facts: KeyFacts = {};
  for (const [key, rawProperty] of Object.entries(schema.properties)) {
    const property = AdvertisedProperty(rawProperty);
    if (property instanceof type.errors) {
      throw new Error(`${where}.${key}: ${property.summary}`);
    }
    facts[key] = {
      required: undeclaredRequired.delete(key),
      types: renderTypes(advertisedTypes(property, `${where}.${key}`)),
      constraints: renderConstraints(
        advertisedConstraints(property, `${where}.${key}`),
      ),
      values: [...(property.enum ?? [])].sort(),
      default: property.default,
    };
  }

  if (undeclaredRequired.size > 0) {
    throw new Error(
      `${where} requires ${[...undeclaredRequired].join(", ")}, which its 'properties' does not declare`,
    );
  }
  return facts;
}

function acceptedFacts(
  props: readonly AcceptedProp[],
  defaults: EnforcedDefaults,
  where: string,
): KeyFacts {
  const unpaired = new Set(defaults.keys());
  const facts: KeyFacts = {};
  for (const prop of props) {
    if (typeof prop.key !== "string") {
      throw new Error(
        `${where} declares a symbol key, which no schema can advertise`,
      );
    }
    unpaired.delete(prop.key);
    facts[prop.key] = {
      required: prop.kind === "required",
      types: renderTypes(acceptedTypes(prop.value, `${where}.${prop.key}`)),
      constraints: renderConstraints(
        acceptedConstraints(prop.value, `${where}.${prop.key}`),
      ),
      values: acceptedValues(prop.value),
      default: UNADVERTISED_FALLBACKS.has(`${where}.${prop.key}`)
        ? undefined
        : defaults.get(prop.key),
    };
  }

  if (unpaired.size > 0) {
    throw new Error(
      `${where} substitutes a value for ${[...unpaired].join(", ")}, which its shape does not declare`,
    );
  }
  return facts;
}

const ADVERTISED_SCHEMAS = new Map(
  TOOL_DEFINITIONS.map((definition) => [
    definition.name,
    definition.inputSchema,
  ]),
);

// Walks a dotted path -- "mail_search.query.header" -- from a tool's
// inputSchema down through the nested 'properties' the segments name.
function advertisedSchemaAt(path: string): unknown {
  const segments = path.split(".");
  const toolName = segments[0];
  if (toolName === undefined) {
    throw new Error("a pairing path names no tool");
  }
  let schema: unknown = ADVERTISED_SCHEMAS.get(toolName);
  if (schema === undefined) {
    throw new Error(`no tool named "${toolName}" is advertised`);
  }
  for (const segment of segments.slice(1)) {
    const parent = AdvertisedObject(schema);
    if (parent instanceof type.errors) {
      throw new Error(`${path}: ${parent.summary}`);
    }
    const child = parent.properties[segment];
    if (child === undefined) {
      throw new Error(`${path}: "${segment}" is not advertised`);
    }
    schema = child;
  }
  return schema;
}

// The seven tool rows come from ARGUMENT_SHAPES, so a tool cannot be added
// without one. The nested rows are named: a nested shape is reached with
// `Type.get`, and `props` is only typed on a Type whose inferred object type
// names its keys, so the descent cannot be written as a loop. 'query' is the
// one nested schema whose shape is not reached this way -- the arguments accept
// it as an opaque object and the handler validates it against SearchQueryArgs
// in a second step -- so the query filters are paired with that shape directly.
//
// A tool row carries the defaults its handler substitutes; the nested rows carry
// none, for the reason NO_ENFORCED_DEFAULTS gives.
const PAIRINGS: [
  path: string,
  props: readonly AcceptedProp[],
  defaults: EnforcedDefaults,
][] = [
  ...Object.entries(ARGUMENT_SHAPES).map(
    ([name, shape]): [string, readonly AcceptedProp[], EnforcedDefaults] => [
      name,
      shape.props,
      enforcedDefaults(name),
    ],
  ),
  [
    "mail_reply.ref",
    ARGUMENT_SHAPES.mail_reply.get("ref").props,
    NO_ENFORCED_DEFAULTS,
  ],
  [
    "mail_read.ref",
    ARGUMENT_SHAPES.mail_read.get("ref").props,
    NO_ENFORCED_DEFAULTS,
  ],
  [
    "mail_flag.ref",
    ARGUMENT_SHAPES.mail_flag.get("ref").props,
    NO_ENFORCED_DEFAULTS,
  ],
  ["mail_search.query", SearchQueryArgs.props, NO_ENFORCED_DEFAULTS],
  ["mail_wait.query", SearchQueryArgs.props, NO_ENFORCED_DEFAULTS],
  [
    "mail_search.query.header",
    SearchQueryArgs.required().get("header").props,
    NO_ENFORCED_DEFAULTS,
  ],
  [
    "mail_wait.query.header",
    SearchQueryArgs.required().get("header").props,
    NO_ENFORCED_DEFAULTS,
  ],
];

describe("mail tool schemas describe what the handlers accept", () => {
  test("every advertised tool has an argument shape, and the reverse", () => {
    expect(Object.keys(ARGUMENT_SHAPES).sort()).toEqual(
      TOOL_DEFINITIONS.map((definition) => definition.name).sort(),
    );
  });

  for (const [path, props, defaults] of PAIRINGS) {
    test(`${path} advertises every key its shape accepts and no other, with the constraints it enforces and the default it applies`, () => {
      const accepted = acceptedFacts(props, defaults, path);
      const advertised = advertisedFacts(advertisedSchemaAt(path), path);

      expect(renderFacts(advertised)).toEqual(renderFacts(accepted));
    });
  }

  // The pairing above holds an advertised default to the value the handler
  // substitutes. This holds it to the shape as well, because the two ways of
  // asking for a default have to agree: a model that reads `default: 0` may
  // send the 0 explicitly on its next call -- the same value, now stated -- and
  // a shape that refuses it turns the documented default into an
  // invalid_arguments error.
  test("every advertised default is a value its own shape accepts", () => {
    const refused: string[] = [];
    for (const [path, props] of PAIRINGS) {
      const shapes = new Map<string | symbol, type.Any>(
        props.map((prop): [string | symbol, type.Any] => [
          prop.key,
          prop.value,
        ]),
      );
      const advertised = advertisedFacts(advertisedSchemaAt(path), path);

      for (const [key, fact] of Object.entries(advertised)) {
        if (fact.default === undefined) continue;
        const shape = shapes.get(key);
        if (shape === undefined) {
          throw new Error(
            `${path}.${key} advertises a default for a key its shape does not declare`,
          );
        }
        // `in` is the side a call supplies, as in acceptedTypes: a default is a
        // value the caller sends, not one a morph produces from it.
        if (!shape.in.allows(fact.default)) {
          refused.push(`${path}.${key}: ${JSON.stringify(fact.default)}`);
        }
      }
    }

    expect(refused).toEqual([]);
  });

  // UNADVERTISED_FALLBACKS is the one list here a hand keeps, and an entry that
  // no longer names a fallback is an exemption granted to nothing -- it would go
  // on excusing whatever key later took the name. So each entry has to name a
  // substitution the handlers still make.
  test("every exempted fallback is one the handlers still apply", () => {
    const applied = new Set(
      [...HANDLER_FACTORIES.keys()].flatMap((tool) =>
        [...enforcedDefaults(tool).keys()].map((key) => `${tool}.${key}`),
      ),
    );

    expect(
      [...UNADVERTISED_FALLBACKS].filter((name) => !applied.has(name)),
    ).toEqual([]);
  });
});

// The tests above hold the advertised keys to the accepted ones. The body of a
// send or a reply carries a constraint over two of those keys that no part of
// the dialect states: exactly one of 'content' and 'payload', and it must be
// the one the declared 'type' takes. 'required' names keys one at a time, so
// listing either would make the other form unadvertisable, and the composition
// keywords that could say it are outside the subset forwarded to every
// inference provider. The tool description is therefore the only place a model
// reads the rule, and these tests hold both halves of that arrangement.
describe("the body constraint the schema cannot express", () => {
  for (const name of ["mail_send", "mail_reply"] as const) {
    test(`${name} advertises neither body field as required`, () => {
      const facts = advertisedFacts(advertisedSchemaAt(name), name);
      for (const key of ["content", "payload"]) {
        const fact = facts[key];
        if (fact === undefined) {
          throw new Error(`${name} does not advertise '${key}'`);
        }
        // Requiring either one would refuse every call using the other, which
        // is the defect a bare 'required' entry would reintroduce.
        expect(fact.required).toBe(false);
      }
    });

    test(`${name} states the body rule in its description`, () => {
      const definition = TOOL_DEFINITIONS.find((d) => d.name === name);
      if (definition === undefined) {
        throw new Error(`no tool named "${name}" is advertised`);
      }
      const description = definition.description;
      expect(description).toContain("content");
      expect(description).toContain("payload");
      // The word the handler's guard depends on being advertised: a call naming
      // no body is refused, and nothing else tells the model so.
      expect(description).toContain("neither");
    });
  }
});
