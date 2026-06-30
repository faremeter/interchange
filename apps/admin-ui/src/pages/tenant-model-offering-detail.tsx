import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Plus, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  modelOfferingDetailQuery,
  deleteModelOfferingMutation,
  updateModelOfferingMutation,
  type UpdateModelOfferingBody,
  offeringPricingQuery,
  createPricingRowMutation,
  type CreatePricingRowBody,
  tenantCatalogModelsQuery,
  tenantModelProvidersQuery,
} from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { CAPABILITIES } from "@/pages/tenant-model-offerings";

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid grid-cols-[160px_1fr] border-b last:border-b-0">
      <dt className="border-r bg-muted/50 px-4 py-3 text-sm font-medium text-muted-foreground">
        {label}
      </dt>
      <dd className="px-4 py-3 text-sm">{children}</dd>
    </div>
  );
}

const FEE_FIELDS = [
  { key: "inputTokenPrice", label: "Input token" },
  { key: "outputTokenPrice", label: "Output token" },
  { key: "cacheReadTokenPrice", label: "Cache read" },
  { key: "cacheWriteTokenPrice", label: "Cache write" },
  { key: "thinkingTokenPrice", label: "Thinking token" },
  { key: "perRequestFee", label: "Per request" },
  { key: "perImageFee", label: "Per image" },
  { key: "perAudioFee", label: "Per audio" },
] as const;

type FeeKey = (typeof FEE_FIELDS)[number]["key"];

export function TenantModelOfferingDetailPage() {
  const { tenantId, offeringId } = useParams({
    from: "/authed/tenants/$tenantId/model-offerings/$offeringId",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: offering, isLoading } = useQuery(
    modelOfferingDetailQuery(tenantId, offeringId),
  );
  const { data: pricing } = useQuery(
    offeringPricingQuery(tenantId, offeringId),
  );
  const { data: models } = useQuery(tenantCatalogModelsQuery(tenantId));
  const { data: providers } = useQuery(tenantModelProvidersQuery(tenantId));

  const modelName = useMemo(
    () => models?.find((m) => m.id === offering?.modelId)?.canonicalName,
    [models, offering],
  );
  const providerName = useMemo(
    () => providers?.find((p) => p.id === offering?.providerId)?.name,
    [providers, offering],
  );

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [priceOpen, setPriceOpen] = useState(false);

  const [editPriority, setEditPriority] = useState("0");
  const [editTags, setEditTags] = useState("");
  const [editCaps, setEditCaps] = useState<string[]>([]);
  const [editDisabled, setEditDisabled] = useState(false);

  const [priceCurrency, setPriceCurrency] = useState("");
  const [priceEffectiveFrom, setPriceEffectiveFrom] = useState("");
  const [priceFees, setPriceFees] = useState<Record<FeeKey, string>>({
    inputTokenPrice: "",
    outputTokenPrice: "",
    cacheReadTokenPrice: "",
    cacheWriteTokenPrice: "",
    thinkingTokenPrice: "",
    perRequestFee: "",
    perImageFee: "",
    perAudioFee: "",
  });

  function enterEditMode() {
    if (!offering) return;
    setEditPriority(String(offering.priority));
    setEditTags(offering.deploymentTags.join(", "));
    setEditCaps(offering.capabilities);
    setEditDisabled(offering.disabled);
    setEditing(true);
  }

  function toggleCap(cap: string) {
    setEditCaps((prev) =>
      prev.includes(cap) ? prev.filter((c) => c !== cap) : [...prev, cap],
    );
  }

  function resetPriceForm() {
    setPriceCurrency("");
    setPriceEffectiveFrom("");
    setPriceFees({
      inputTokenPrice: "",
      outputTokenPrice: "",
      cacheReadTokenPrice: "",
      cacheWriteTokenPrice: "",
      thinkingTokenPrice: "",
      perRequestFee: "",
      perImageFee: "",
      perAudioFee: "",
    });
  }

  const updateMut = useMutation({
    ...updateModelOfferingMutation(tenantId, offeringId, queryClient),
    onSuccess: () => {
      void updateModelOfferingMutation(
        tenantId,
        offeringId,
        queryClient,
      ).onSuccess();
      void queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "model-offerings", offeringId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteModelOfferingMutation(tenantId, offeringId, queryClient),
    onSuccess: () => {
      void deleteModelOfferingMutation(
        tenantId,
        offeringId,
        queryClient,
      ).onSuccess();
      void navigate({
        to: "/tenants/$tenantId/model-offerings",
        params: { tenantId },
      });
    },
  });

  const priceMut = useMutation({
    ...createPricingRowMutation(tenantId, offeringId, queryClient),
    onSuccess: () => {
      void createPricingRowMutation(
        tenantId,
        offeringId,
        queryClient,
      ).onSuccess();
      setPriceOpen(false);
      resetPriceForm();
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!offering) return;
    const tags = editTags
      .split(",")
      .map((t) => t.trim())
      .filter((t) => t.length > 0);
    const priority = Number.parseInt(editPriority, 10);
    const body: UpdateModelOfferingBody = {
      priority: Number.isNaN(priority) ? 0 : priority,
      deploymentTags: tags,
      capabilities: editCaps,
      disabled: editDisabled,
    };
    updateMut.mutate(body);
  }

  function handleAddPrice(e: React.FormEvent) {
    e.preventDefault();
    const body: CreatePricingRowBody = { currency: priceCurrency.trim() };
    if (priceEffectiveFrom.trim()) {
      body.effectiveFrom = new Date(priceEffectiveFrom).toISOString();
    }
    for (const { key } of FEE_FIELDS) {
      const value = priceFees[key].trim();
      if (value) body[key] = value;
    }
    priceMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!offering) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/model-offerings"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Model Offerings
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">
            {modelName ?? offering.modelId} @{" "}
            {providerName ?? offering.providerId}
          </h2>
        </div>
        <div className="flex items-center gap-2">
          {editing ? (
            <Button variant="ghost" size="sm" onClick={() => setEditing(false)}>
              <X className="size-4" />
              Cancel
            </Button>
          ) : (
            <>
              <Button variant="outline" size="sm" onClick={enterEditMode}>
                <Pencil className="size-4" />
                Edit
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setDeleteOpen(true)}
                className="text-destructive hover:text-destructive"
              >
                <Trash2 className="size-4" />
                Delete
              </Button>
            </>
          )}
        </div>
      </div>

      <div className="mt-6">
        {editing ? (
          <form onSubmit={handleSave}>
            <div className="overflow-hidden rounded-lg border">
              <div className="grid grid-cols-[160px_1fr] border-b">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label htmlFor="edit-priority">Priority</Label>
                </div>
                <div className="px-4 py-2">
                  <Input
                    id="edit-priority"
                    type="number"
                    value={editPriority}
                    onChange={(e) => setEditPriority(e.target.value)}
                  />
                </div>
              </div>
              <div className="grid grid-cols-[160px_1fr] border-b">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label>Capabilities</Label>
                </div>
                <div className="flex flex-wrap gap-2 px-4 py-2">
                  {CAPABILITIES.map((cap) => (
                    <Button
                      key={cap}
                      type="button"
                      size="sm"
                      variant={editCaps.includes(cap) ? "default" : "outline"}
                      onClick={() => toggleCap(cap)}
                    >
                      {cap}
                    </Button>
                  ))}
                </div>
              </div>
              <div className="grid grid-cols-[160px_1fr] border-b">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label htmlFor="edit-tags">Deployment tags</Label>
                </div>
                <div className="px-4 py-2">
                  <Input
                    id="edit-tags"
                    value={editTags}
                    onChange={(e) => setEditTags(e.target.value)}
                    placeholder="Comma-separated, optional"
                  />
                </div>
              </div>
              <div className="grid grid-cols-[160px_1fr]">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label>Status</Label>
                </div>
                <div className="px-4 py-2">
                  <Select
                    value={editDisabled ? "disabled" : "enabled"}
                    onValueChange={(v) => setEditDisabled(v === "disabled")}
                  >
                    <SelectTrigger className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="enabled">Enabled</SelectItem>
                      <SelectItem value="disabled">Disabled</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>
            <MutationError error={updateMut.error} />
            <div className="mt-4">
              <Button type="submit" disabled={updateMut.isPending}>
                {updateMut.isPending ? "Saving..." : "Save Changes"}
              </Button>
            </div>
          </form>
        ) : (
          <dl className="overflow-hidden rounded-lg border">
            <Row label="Model">{modelName ?? offering.modelId}</Row>
            <Row label="Provider">{providerName ?? offering.providerId}</Row>
            <Row label="Priority">{offering.priority}</Row>
            <Row label="Capabilities">
              {offering.capabilities.length === 0 ? (
                "-"
              ) : (
                <div className="flex flex-wrap gap-1">
                  {offering.capabilities.map((cap) => (
                    <Badge key={cap} variant="outline">
                      {cap}
                    </Badge>
                  ))}
                </div>
              )}
            </Row>
            <Row label="Deployment tags">
              {offering.deploymentTags.length === 0
                ? "-"
                : offering.deploymentTags.join(", ")}
            </Row>
            <Row label="Status">
              {offering.disabled ? (
                <Badge variant="secondary">Disabled</Badge>
              ) : (
                <Badge>Enabled</Badge>
              )}
            </Row>
          </dl>
        )}
      </div>

      {/* Pricing history */}
      <div className="mt-8">
        <div className="flex items-center justify-between">
          <h3 className="text-base font-semibold">Pricing</h3>
          <Button
            size="sm"
            variant="outline"
            onClick={() => setPriceOpen(true)}
          >
            <Plus className="size-4" />
            Add Price
          </Button>
        </div>
        {pricing && pricing.length > 0 ? (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Currency</TableHead>
                  <TableHead>Input</TableHead>
                  <TableHead>Output</TableHead>
                  <TableHead>Effective from</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pricing.map((row) => (
                  <TableRow key={row.id}>
                    <TableCell className="font-medium">
                      {row.currency}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {row.inputTokenPrice ?? "-"}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {row.outputTokenPrice ?? "-"}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {new Date(row.effectiveFrom).toLocaleString()}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <p className="mt-3 text-sm text-muted-foreground">
            No pricing rows yet.
          </p>
        )}
      </div>

      {/* Add price dialog */}
      <Dialog
        open={priceOpen}
        onOpenChange={(open) => {
          setPriceOpen(open);
          if (!open) resetPriceForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Pricing Row</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleAddPrice} className="grid gap-4">
            <div className="grid gap-2">
              <Label htmlFor="price-currency">Currency</Label>
              <Input
                id="price-currency"
                value={priceCurrency}
                onChange={(e) => setPriceCurrency(e.target.value)}
                placeholder="e.g. USD"
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="price-effective">Effective from</Label>
              <Input
                id="price-effective"
                type="datetime-local"
                value={priceEffectiveFrom}
                onChange={(e) => setPriceEffectiveFrom(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Defaults to now if left blank.
              </p>
            </div>
            <div className="grid grid-cols-2 gap-3">
              {FEE_FIELDS.map(({ key, label }) => (
                <div key={key} className="grid gap-2">
                  <Label htmlFor={`price-${key}`}>{label}</Label>
                  <Input
                    id={`price-${key}`}
                    value={priceFees[key]}
                    onChange={(e) =>
                      setPriceFees((prev) => ({
                        ...prev,
                        [key]: e.target.value,
                      }))
                    }
                    placeholder="Optional"
                  />
                </div>
              ))}
            </div>
            <MutationError error={priceMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={priceMut.isPending || !priceCurrency.trim()}
              >
                {priceMut.isPending ? "Adding..." : "Add Price"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete offering?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently deletes the offering and its pricing history.
              Running instances resolved through it fail over to the next
              eligible source. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <MutationError error={deleteMut.error} />
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => deleteMut.mutate()}
              disabled={deleteMut.isPending}
            >
              {deleteMut.isPending ? "Deleting..." : "Delete"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
