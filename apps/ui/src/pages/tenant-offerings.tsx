import { useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import {
  createOfferingMutation,
  tenantAgentsQuery,
  tenantOfferingsQuery,
} from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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

export function TenantOfferingsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data: offerings, isLoading } = useQuery(
    tenantOfferingsQuery(tenantId),
  );
  const { data: agents } = useQuery(tenantAgentsQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [createAgentId, setCreateAgentId] = useState("");
  const [createName, setCreateName] = useState("");
  const [createDescription, setCreateDescription] = useState("");

  function resetCreateForm() {
    setCreateAgentId("");
    setCreateName("");
    setCreateDescription("");
  }

  const createMut = useMutation({
    ...createOfferingMutation(tenantId, queryClient),
    onSuccess: () => {
      createOfferingMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  return (
    <div>
      <TenantNav />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Offerings</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Add Offering
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : offerings?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No offerings registered.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Agent</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Pricing</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {offerings?.map((ofr) => (
                <TableRow
                  key={ofr.id}
                  className="cursor-pointer"
                  onClick={() =>
                    navigate({
                      to: "/tenants/$tenantId/offerings/$offeringId",
                      params: { tenantId, offeringId: ofr.id },
                    })
                  }
                >
                  <TableCell className="font-medium">{ofr.name}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">{ofr.agentName}</Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {ofr.description ?? "-"}
                  </TableCell>
                  <TableCell>
                    {ofr.pricing?.base ? (
                      <span className="font-mono text-xs">
                        {ofr.pricing.base.amount} {ofr.pricing.base.currency}
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground">
                        Free
                      </span>
                    )}
                    {ofr.pricing?.negotiable ? (
                      <Badge variant="outline" className="ml-1">
                        negotiable
                      </Badge>
                    ) : null}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {/* Create dialog */}
      <Dialog
        open={createOpen}
        onOpenChange={(open) => {
          setCreateOpen(open);
          if (!open) resetCreateForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Offering</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: {
                agentId: string;
                name: string;
                description?: string;
              } = {
                agentId: createAgentId,
                name: createName.trim(),
              };
              if (createDescription.trim())
                body.description = createDescription.trim();
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label>Agent</Label>
              <Select value={createAgentId} onValueChange={setCreateAgentId}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select an agent" />
                </SelectTrigger>
                <SelectContent>
                  {agents?.map((a) => (
                    <SelectItem key={a.id} value={a.id}>
                      {a.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="ofr-name">Name</Label>
              <Input
                id="ofr-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                required
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="ofr-description">Description</Label>
              <Input
                id="ofr-description"
                value={createDescription}
                onChange={(e) => setCreateDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending || !createAgentId || !createName.trim()
                }
              >
                {createMut.isPending ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
